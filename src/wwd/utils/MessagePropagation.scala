package wwd.utils

import breeze.math.MutablizingAdaptor.Lambda2
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import wwd.entity.{InfluenceEdgeAttr, VertexAttr, EdgeAttr}
import scala.collection.Seq
import scala.reflect.ClassTag

/**
  * Created by weiwenda on 2017/3/20.
  */
object MessagePropagation {
    type Path = Seq[(VertexId, String, Double, Double,EdgeAttr)]
    type Paths = Seq[Seq[(VertexId, String, Double, Double,EdgeAttr)]]

    def computeCI(srclist: Seq[(String, Double)], dstlist: Seq[(String, Double)], kind: Int): Double = {

        var score = 0.0
        val srcMap = srclist.toMap
        val dstMap = dstlist.toMap
        if (kind == 1) {
            srcMap.keys.toSeq.intersect(dstMap.keys.toSeq).foreach(key =>
                score += srcMap(key).min(dstMap(key))
            )
        } else if (kind == 2) {
            score = srcMap.keys.toSeq.intersect(dstMap.keys.toSeq).size
        }
        score
    }

    //annotation of david:在3个分数之间使用最大值做为控制人亲密度
    def computeCI(srcAttr: VertexAttr, dstAttr: VertexAttr, kind: Int = 1): Double = {
//        val gd_score = computeCI(srcAttr.gd_list, dstAttr.gd_list, kind)
//        val zrrtz_score = computeCI(srcAttr.zrrtz_list, dstAttr.zrrtz_list, kind)
        val gd_score = 0D
        val zrrtz_score = 0D
        var fddbr_score = 0D
        if (srcAttr.fddbr.equals(dstAttr.fddbr)) fddbr_score = 1D
        val toReturn = gd_score.max(zrrtz_score).max(fddbr_score)
        toReturn
    }
//
//    //annotation of david:归一化企业相关自然人的权重
//    def fixVertexWeight(tpin: Graph[VertexAttr, EdgeAttr]) = {
//        val toReurn = tpin.mapVertices { case (vid, vattr) =>
//            val sum_gd = vattr.gd_list.map(_._2).sum
//            vattr.gd_list = vattr.gd_list.map { case (gd, weight) => (gd, weight / sum_gd) }
//            val sum_tz = vattr.zrrtz_list.map(_._2).sum
//            vattr.zrrtz_list = vattr.zrrtz_list.map { case (tzf, weight) => (tzf, weight / sum_tz) }
//            vattr
//        }
//        toReurn
//    }


    //annotation of david:概率上限
    def computePl(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double,method:String="maxmin") = {
        var max = 0D
        method match{
            case "maxmin"=> max = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).max;
            case "proba" => max = controllerInterSect+ tz_bl + kg_bl + jy_bl -
                controllerInterSect*tz_bl - tz_bl*kg_bl - kg_bl* jy_bl - controllerInterSect*kg_bl - controllerInterSect* jy_bl -tz_bl*jy_bl +
                controllerInterSect * tz_bl * kg_bl +controllerInterSect*kg_bl*jy_bl + controllerInterSect * tz_bl*jy_bl + tz_bl*kg_bl*jy_bl -
                controllerInterSect * tz_bl * kg_bl * jy_bl;
            case "bouned" => max = (controllerInterSect+tz_bl+kg_bl+jy_bl).min(1);
            case "ds" => max =1- (1-controllerInterSect)*(1-tz_bl)*(1-kg_bl)*(1-jy_bl)/2
        }
        if (max > 1)
            max = 1
        max
    }

    //annotation of david:概率下限 对空集进行min会报empty.min错
    def computeBel(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double,method :String = "maxmin") = {
        val tmp = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).filter(_ > 0)
        var min = 0D
        method match {
            case "maxmin" =>
                if (tmp.size > 0)
                    min = tmp.min
            case "proba" =>  if (tmp.size > 0) min = tmp.reduce(_*_);
            case "bouned" => min = (controllerInterSect+tz_bl+kg_bl+jy_bl-1).max(0);
            case "ds" => min = 1-(1-controllerInterSect)*(1-tz_bl)*(1-kg_bl)*(1-jy_bl)
        }
        if (min > 1)
            min = 1
        min
    }

    //annotation of david:决定路径经过哪些邻居,ps:每个企业只影响3家企业
    def selectNeighbor[VD: ClassTag](belAndPl: Graph[VD, InfluenceEdgeAttr], selectTopN: Int = 3): Graph[VD, InfluenceEdgeAttr] = {
        def sendMessage(edge: EdgeContext[VD, InfluenceEdgeAttr, Seq[(VertexId, InfluenceEdgeAttr)]]): Unit = {
            edge.sendToSrc(Seq((edge.dstId, edge.attr)))
        }
        val messages = belAndPl.aggregateMessages[Seq[(VertexId, InfluenceEdgeAttr)]](sendMessage(_), _ ++ _).cache()

        val filtered_edges = messages.map { case (vid, edgelist) =>
                (vid, edgelist.sortBy(_._2.pl)(Ordering[Double].reverse).slice(0, selectTopN))
        }.flatMap { case (vid, edgelist) => edgelist.map(e => Edge(vid, e._1, e._2)) }


        Graph[VD, InfluenceEdgeAttr](belAndPl.vertices, filtered_edges).persist()
    }

    //annotation of david:收集所有长度initlength-1到maxIteration-1的路径
    def getPath(graph: Graph[Paths, InfluenceEdgeAttr], maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
        // 发送路径
        def sendPaths(edge: EdgeContext[Paths, InfluenceEdgeAttr, Paths],
                      length: Int): Unit = {
            val satisfied = edge.dstAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.srcId))
            if (satisfied.size > 0) {
                // 向终点发送顶点路径集合
                edge.sendToSrc(satisfied.map(Seq((edge.srcId, edge.attr.src, edge.attr.bel, edge.attr.pl,edge.attr.edgeAttr)) ++ _))
            }
        }
        var preproccessedGraph = graph.cache()
        var i = initLength
        var messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _)
        var activeMessages = messages.count()
        var prevG: Graph[Paths, InfluenceEdgeAttr] = null
        while (activeMessages > 0 && i <= maxIteratons) {
            prevG = preproccessedGraph
            preproccessedGraph = preproccessedGraph.joinVertices[Paths](messages)((id, vd, path) => vd ++ path).cache()
            print("iterator " + i + " finished! ")
            i += 1
            val oldMessages = messages
            messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _).cache()
            try{
                activeMessages = messages.count()
            }catch{
                case ex:Exception =>
                    println("又发生异常了")
            }
            oldMessages.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            prevG.edges.unpersist(blocking = false)
        }
        //         printGraph[Paths,Int](preproccessedGraph)
        preproccessedGraph.vertices
    }

    //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
    def run(tpin: Graph[VertexAttr, EdgeAttr], sqlContext: SQLContext, bypass: Boolean = false,method:String="maxmin", lambda: Int = 1) = {
        // tpin size: vertices:93523 edges:633300
        val belAndPl = tpin.mapTriplets { case triplet =>
            //            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            //annotation of david:bel为概率下限，pl为概率上限
            val bel = computeBel(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl,method)
            val pl = computePl(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl,method)
            val attr = InfluenceEdgeAttr(bel, pl, triplet.srcAttr.nsrdzdah, triplet.dstAttr.nsrdzdah)
            attr.edgeAttr = triplet.attr
            attr
        }
        val simplifiedGraph = selectNeighbor[String](belAndPl.mapVertices((vid, vattr) => vattr.nsrdzdah))
        //annotation of david:企业对自身的bel和pl均为1
        val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, nsrdzdah, 1.0, 1.0,EdgeAttr()))) }
        //initGraph size: vertices:93523 edges:132965
        val paths = getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)
        //        paths.saveAsObjectFile("/tpin/wwd/influence/paths")
        //        sc.objectFile[(VertexId,MessagePropagation.Paths)](verticesFilePath)
        //paths:93523

        //annotation of david:使用第一种三角范式
        val influenceEdge = influenceOnPath(paths, lambda, sqlContext, bypass)
        val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

        //annotation of david:滤除影响力过小的边
        val finalInfluenceGraph = influenceInTotal(influenceGraph)
        finalInfluenceGraph
        //finalInfluenceGraph size: vertices:93523 edges:1850050
    }

    //annotation of david:模糊逻辑
    def rule(s: String, bl: Double,point:Double) = {
        var toReturn=0D
//        val norm = if(point>=0.8) 1 else if(point <= 0.2 ) 0 else  1/0.6 * point - 0.334
        val norm = if(point>=0.8) 1  else  1.25 * point
        s match {
            case "il" =>
                if(bl>=0.1)
                    toReturn = norm
                else
                    toReturn = if (norm < 10*bl) norm else 10*bl
            case "tz" =>
                if(bl>=0.2)
                    toReturn = norm
                else
                    toReturn = if (norm < 5*bl) norm else 5*bl
            case "jy" =>
                if(bl>=0.1)
                    toReturn = norm
                else
                    toReturn = if (norm < 10*bl) norm else 10*bl
        }
        toReturn
    }

    def computeMembership(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double, point:Double): Double = {
        //annotation of david:四条规则取最大
        Seq(rule("il",il_bl,point),rule("tz",tz_bl,point),rule("tz",kg_bl,point),rule("jy",jy_bl,point)).max
    }

    def computeFuzzScore(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
        val score:Array[Double] = new Array[Double](11)
        //annotation of david:采样
        for(i <- Range(0,11)){
            score(i) = computeMembership(il_bl,tz_bl,kg_bl ,jy_bl,i/10D)
        }
        //annotation of david:反模糊化 :最大平均去模糊化 VS 重心面积中心去模糊 VS 最大隶属度法
        val result = score.zipWithIndex.sortWith((a,b)=> if(a._1==b._1 ) (b._2-a._2)>0 else (a._1-b._1)>0)(0)._2/10D
//        val result = score.sum/score.size
//        val result = score.zipWithIndex.aggregate(0D)({case (cur,(score,index))=>
//            cur +score * index/10D
//        },_+_)/score.sum
        result
    }

    //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
    def runFuzz(tpin: Graph[VertexAttr, EdgeAttr], sqlContext: SQLContext, lambda: Int = 1,bypass: Boolean = false) = {
        // tpin size: vertices:93523 edges:633300
        val belAndPl = tpin.mapTriplets { case triplet =>
            //            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            //annotation of david:bel为概率下限，pl为概率上限
            val bel = computeFuzzScore(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
            val attr = InfluenceEdgeAttr(bel, bel, triplet.srcAttr.nsrdzdah, triplet.dstAttr.nsrdzdah)
            attr.edgeAttr = triplet.attr
            attr
        }
        val simplifiedGraph = selectNeighbor[String](belAndPl.mapVertices((vid, vattr) => vattr.nsrdzdah))
        //annotation of david:企业对自身的bel和pl均为1
        val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, nsrdzdah, 1.0, 1.0,EdgeAttr()))) }
        //initGraph size: vertices:93523 edges:132965
        val paths = getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)

        //annotation of david:使用第一种三角范式
        val influenceEdge = influenceOnPath(paths, lambda, sqlContext, bypass)
        val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

        //annotation of david:滤除影响力过小的边
        val finalInfluenceGraph = influenceInTotal(influenceGraph)
        finalInfluenceGraph
        //finalInfluenceGraph size: vertices:93523 edges:1850050
    }

    //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
    def runTidalTrust(tpin: Graph[VertexAttr, EdgeAttr], sqlContext: SQLContext,method:String="maxmin", lambda: Int = 1) = {
        // tpin size: vertices:93523 edges:633300
        val belAndPl = tpin.mapTriplets { case triplet =>
            //            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            //annotation of david:bel为概率下限，pl为概率上限
            val bel = computeBel(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl,method)
            val pl = computePl(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl,method)
            val attr = InfluenceEdgeAttr(bel, pl, triplet.srcAttr.nsrdzdah, triplet.dstAttr.nsrdzdah)
            attr.edgeAttr = triplet.attr
            attr
        }
        val simplifiedGraph = selectNeighbor[String](belAndPl.mapVertices((vid, vattr) => vattr.nsrdzdah))
        //annotation of david:企业对自身的bel和pl均为1
        val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, nsrdzdah, 1.0, 1.0,EdgeAttr()))) }
        //initGraph size: vertices:93523 edges:132965
        val paths = getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)
        //        paths.saveAsObjectFile("/tpin/wwd/influence/paths")
        //        sc.objectFile[(VertexId,MessagePropagation.Paths)](verticesFilePath)
        //paths:93523

        //annotation of david:使用第一种三角范式
        val influenceEdge = influenceOnPathTidalTrust(paths, lambda, sqlContext)
        val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

        //annotation of david:滤除影响力过小的边
        val finalInfluenceGraph = influenceInTotal(influenceGraph)
        finalInfluenceGraph
        //finalInfluenceGraph size: vertices:93523 edges:1850050
    }

    //annotation of david:针对图结构的shared segment和crossing segment进行修改TODO
    def graphReduce[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double,EdgeAttr)]]](vattr: T): T = {
        vattr
    }

    //annotation of david:使用frank t-norm聚合路径上的影响值，vid,bel,pl
    def combineInfluence(x: (graphx.VertexId, String, Double, Double,EdgeAttr), y: (graphx.VertexId, String, Double, Double,EdgeAttr), lambda: Int) = {
        val plambda = 0.001
        val a = x._4
        val b = y._4
        var pTrust = 0D
        val unc = x._4 + y._4 - y._3
        if (lambda == 0) pTrust = a.min(b)
        else if (lambda == 1) pTrust = a * b
//        else if (lambda == Integer.MAX_VALUE) pTrust = (a + b - 1).max(0.0)
        else if (lambda == 3) pTrust = (a + b - 1).max(0.0)
        else if (lambda == 2) pTrust = (a * b) / (a + b - a * b)
        else pTrust = Math.log(1 + (((Math.pow(plambda, a) - 1) * ((Math.pow(plambda, b) - 1))) / (plambda - 1))) / Math.log(plambda)
        (y._1, "", pTrust, unc,y._5)
    }

    def combinePath1(x: Double, y: Double) = {
        if(x>y) x else y
    }
    def combinePath2(x: Double, y: Double) = {
        if(x>y) x else y
    }
    //annotation of david:利用pTrust和unc聚合多路径的影响值，权重比例为 1-unc
    def combinePath1(x: (Double, Double), y: (Double, Double)) = {
        (x._1 + (y._1 * (1 - y._2)), x._2 + 1 - y._2)
    }

    //annotation of david:利用pTrust和unc聚合多路径的影响值
    def combinePath2(x: (Double, Double), y: (Double, Double)) = {
        (x._1 + y._1, x._2 + y._2)
    }
    //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
    def influenceOnPathTidalTrust[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double,EdgeAttr)]]](paths: RDD[(VertexId, T)], lambda: Int, sqlContext: SQLContext) = {
        val influences = paths.map { case (vid, vattr) =>
            val DAG = graphReduce(vattr)
            val influenceSinglePath = DAG.map { path =>
                val res = path.reduceLeft((a, b) => combineInfluence(a, b, lambda))
                //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
                (res._1, res._3)
            }
            (vid, influenceSinglePath)
        }
            .flatMap { case (vid, list) =>
                list.map { case (dstid, pTrust) => ((vid, dstid), pTrust) }
            }.aggregateByKey((0D))(combinePath1, combinePath2).
            map { case ((vid, dstid), pTrust) => Edge(vid, dstid, pTrust ) }
        influences
    }
    //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
    def influenceOnPath[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double,EdgeAttr)]]](paths: RDD[(VertexId, T)], lambda: Int, sqlContext: SQLContext, bypass: Boolean) = {
        if (!bypass) {
            val toOutput = paths.flatMap { case (vid, vattr) =>
                val DAG = graphReduce(vattr)
                DAG.filter { case path =>
                    val res = path.reduceLeft((a, b) => combineInfluence(a, b, lambda))
                    //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
                    // 只输出大于0.1pTrust的路径
                    res._3 > 0.1
                }
            }
            OracleDBUtil.savePath(toOutput, sqlContext)
        }
        val influences = paths.map { case (vid, vattr) =>
            val DAG = graphReduce(vattr)
            val influenceSinglePath = DAG.map { path =>
                val res = path.reduceLeft((a, b) => combineInfluence(a, b, lambda))
                //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
                (res._1, res._3, res._4 / (path.size - 1))
            }
            (vid, influenceSinglePath)
        }
            .flatMap { case (vid, list) =>
                list.map { case (dstid, pTrust, unc) => ((vid, dstid), (pTrust, unc)) }
            }.aggregateByKey((0D, 0D))(combinePath1, combinePath2).
            map { case ((vid, dstid), (pTrust, total_certainty)) => Edge(vid, dstid, pTrust / total_certainty) }
        influences
    }

    //annotation of david:增加考虑源点企业和终点企业的控制人亲密度，以及多条路径，得到标量的影响值，加权平均
    def influenceInTotal(influenceGraph: Graph[VertexAttr, Double]) = {
        val toReturn = influenceGraph.mapTriplets { case triplet =>
            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            triplet.attr.max(controllerInterSect)
        }.subgraph(epred = triplet => triplet.attr > 0.01)
        toReturn
    }

}

package wwd.utils

import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import wwd.entity.{ InfluenceEdgeAttr, VertexAttr, EdgeAttr}
import scala.collection.Seq

/**
  * Created by weiwenda on 2017/3/20.
  */
object MessagePropagation {
    type Path = Seq[(VertexId, Double, Double)]
    type Paths = Seq[Seq[(VertexId, Double, Double)]]

    def computeCI(srclist: Seq[(String, Double)], dstlist: Seq[(String, Double)]): Double = {

        var score = 0.0
        val srcMap = srclist.toMap
        val dstMap = dstlist.toMap
        srcMap.keys.toSeq.intersect(dstMap.keys.toSeq).foreach(key =>
            score += srcMap(key).min(dstMap(key))
        )
        score
    }

    //annotation of david:在3个分数之间使用最大值做为控制人亲密度
    def computeCI(srcAttr: VertexAttr, dstAttr: VertexAttr): Double = {
        val gd_score = computeCI(srcAttr.gd_list, dstAttr.gd_list)
        val zrrtz_score = computeCI(srcAttr.zrrtz_list, dstAttr.zrrtz_list)
        var fddbr_score = 0D
        if (srcAttr.fddbr.equals(dstAttr.fddbr)) fddbr_score = 1D
        val toReturn = gd_score.max(zrrtz_score).max(fddbr_score)
        toReturn
    }

    //annotation of david:修正企业相关自然人的权重
    def fixVertexWeight(tpin: Graph[VertexAttr, EdgeAttr]) = {
        val toReurn = tpin.mapVertices { case (vid, vattr) =>
            val sum_gd = vattr.gd_list.map(_._2).sum
            val sum_tz = vattr.zrrtz_list.map(_._2).sum
            vattr.gd_list = vattr.gd_list.map { case (gd, weight) => (gd, weight / sum_gd) }
            vattr.zrrtz_list = vattr.zrrtz_list.map { case (tzf, weight) => (tzf, weight / sum_tz) }
            vattr
        }
        toReurn
    }


    def computePl(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
        var sum = controllerInterSect + tz_bl + kg_bl + jy_bl
        if (sum > 1)
            sum = 1
        sum
    }

    def computeBel(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
        var max = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).max
        if (max > 1)
            max = 1
        max
    }

    //annotation of david:决定路径经过哪些邻居,ps:每个企业只影响3家企业
    def selectNeighbor(belAndPl: Graph[Int, InfluenceEdgeAttr]) = {
        val selectTopN = 3
        def sendMessage(edge: EdgeContext[Int, InfluenceEdgeAttr, Seq[(VertexId, InfluenceEdgeAttr)]]): Unit = {
            edge.sendToSrc(Seq((edge.dstId, edge.attr)))
        }
        val messages = belAndPl.aggregateMessages[Seq[(VertexId, InfluenceEdgeAttr)]](sendMessage(_), _ ++ _)
        val filtered_edges = messages.map { case (vid, edgelist) =>
            (vid, edgelist.sortBy(_._2.bel)(Ordering[Double].reverse).slice(0, 3))
        }.flatMap { case (vid, edgelist) => edgelist.map(e => Edge(vid, e._1, e._2)) }
        Graph(belAndPl.vertices, filtered_edges).persist()
    }

    //annotation of david:收集所有长度initlength-1到maxIteration-1的路径
    def getPath(graph: Graph[Paths, InfluenceEdgeAttr], maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
        // 发送路径
        def sendPaths(edge: EdgeContext[Paths, InfluenceEdgeAttr, Paths],
                      length: Int): Unit = {
            val satisfied = edge.srcAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.dstId))
            if (satisfied.size > 0) {
                // 向终点发送顶点路径集合
                edge.sendToDst(satisfied.map(_ ++ Seq((edge.dstId, edge.attr.bel, edge.attr.pl))))
            }
        }
        // 路径长度
        val degreesRDD = graph.degrees.cache()
        // 使用度大于0的顶点和边构建图
        var preproccessedGraph = graph
            .outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0)))
            .subgraph(vpred = {
                case (vid, (vattr, degreesVar)) =>
                    degreesVar > 0
            }
            )
            .mapVertices { case (vid, (list, degreesVar)) => list }
            .cache()
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
            activeMessages = messages.count()
            oldMessages.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            prevG.edges.unpersist(blocking = false)
        }
        //         printGraph[Paths,Int](preproccessedGraph)
        preproccessedGraph.vertices
    }

    //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
    def run(tpin: Graph[VertexAttr, EdgeAttr]) = {
        val belAndPl = fixVertexWeight(tpin).mapTriplets { case triplet =>
            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            //annotation of david:bel为概率下限，pl为概率上限
            val bel = computeBel(controllerInterSect, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
            val pl = computePl(controllerInterSect, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
            InfluenceEdgeAttr(bel, pl)
        }
        val simplifiedGraph = selectNeighbor(belAndPl.mapVertices((vid, vattr) => 1))
        val initGraph = simplifiedGraph.mapVertices { case (vid, useless) => Seq(Seq((vid, 1.0, 0.0))) }
        val paths = getPath(initGraph, 4, 1).flatMap(_._2).filter(_.size>1).groupBy(e=>e.head._1)
        val influenceEdge = influenceOnPath(paths, 1)
        val influenceGraph = Graph(belAndPl.vertices,influenceEdge).persist()
        val finalInfluenceGraph =influenceInTotal(influenceGraph)
        finalInfluenceGraph
    }

    //annotation of david:针对图结构的shared segment和crossing segment进行修改TODO
    def graphReduce[T<:Iterable[Seq[(graphx.VertexId, Double, Double)]]](vattr:T):T= {
        vattr
    }

    //annotation of david:使用frank t-norm聚合路径上的影响值
    def combineInfluence(x: (graphx.VertexId, Double, Double), y: (graphx.VertexId, Double, Double), lambda: Int) = {
        val a = x._2
        val b = y._2
        var pTrust = 0D
        val unc = x._3 + y._3 - y._2
        if (lambda == 0) pTrust = a.min(b)
        else if (lambda == 1) pTrust = a * b
        else if (lambda == Integer.MAX_VALUE) pTrust = (a + b - 1).max(0.0)
        else pTrust = Math.log(1 + (((Math.pow(lambda, a) - 1) * ((Math.pow(lambda, b) - 1))) / (lambda - 1))) / Math.log(lambda)
        (y._1, pTrust, unc)
    }

    //annotation of david:利用pTrust和unc聚合多路径的影响值，权重比例为 1-unc
    def combinePath1(x: (Double, Double), y: (Double, Double)) = {
        (x._1+(y._1*(1-y._2)),x._2+1-y._2)
    }
    //annotation of david:利用pTrust和unc聚合多路径的影响值
    def combinePath2(x: (Double, Double), y: (Double, Double)) = {
        (x._1+y._1,x._2+y._2)
    }

    //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
    def influenceOnPath[T<:Iterable[Seq[(graphx.VertexId, Double, Double)]]](paths: RDD[(VertexId,T)], lambda: Int) = {
        val influences = paths.map{case (vid, vattr) =>
                val DAG = graphReduce(vattr)
                val influenceSinglePath = DAG.map { path =>
                    val res = path.reduceLeft((a, b) => combineInfluence(a, b, lambda))
                    //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
                    (res._1,res._2,res._3/(path.size-1))
                }
                (vid,influenceSinglePath)
            }
           .flatMap { case (vid, list) =>
                list.map { case (dstid, pTrust, unc) => ((vid, dstid), (pTrust, unc)) }
            }.aggregateByKey((0D,0D))(combinePath1,combinePath2).
            map{case ((vid,dstid),(pTrust,total_certainty))=> Edge(vid,dstid,pTrust/total_certainty) }
        influences
    }

    //annotation of david:增加考虑源点企业和终点企业的控制人亲密度，以及多条路径，得到标量的影响值，加权平均
    def influenceInTotal(influenceGraph: Graph[VertexAttr, Double]) = {
        val toReturn = influenceGraph.mapTriplets { case triplet =>
            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
            triplet.attr.max(controllerInterSect)
        }
        toReturn
    }

}

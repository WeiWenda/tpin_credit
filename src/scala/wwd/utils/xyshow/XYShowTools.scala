package wwd.utils.xyshow

import org.apache.spark.graphx._
import wwd.entity.impl._
import wwd.entity.{VertexAttr}

/**
  * Created by weiwenda on 2017/5/11.
  */
object XYShowTools {

    //annotation of david:将传统的tpin转换为影响力初始网络
    def transform(tpinWithIL: Graph[WholeVertexAttr, WholeEdgeAttr]) = {
        //        def sendPaths(edge: EdgeContext[WholeVertexAttr,WholeEdgeAttr,Influence]): Unit = {
        //            // 起点为自然人，而终点为企业
        //            if ( !edge.dstAttr.ishuman && edge.srcAttr.ishuman){
        //                // 向终点发送顶点路径集合
        //                edge.sendToDst(Seq((edge.srcId,edge.attr.w_control,edge.attr.w_gd,edge.attr.w_tz)))
        //            }
        //        }
        //        //annotation of david:至少应有法人信息
        //        val Relationships = tpinWithIL.aggregateMessages[Influence](sendPaths, _ ++ _).filter(_._2.filter(_._2>0).size>0)
        //        val newVertices = tpinWithIL.vertices.join(Relationships).map{case (vid,(wholeVertexAttr,seq))=>
        //            val fddbr = seq.filter(_._2>0)(0)._1
        //            val vattr = VertexAttr(wholeVertexAttr.nsrsbh,fddbr.toString)
        //            vattr.gd_list = seq.filter(_._3>0).map(e=> (e._1.toString,e._3))
        //            vattr.zrrtz_list = seq.filter(_._4>0).map(e=> (e._1.toString,e._4))
        //            vattr.xydj = wholeVertexAttr.xydj
        //            vattr.xyfz = wholeVertexAttr.xyfz
        //            (vid,vattr)
        //        }
        val toReturn = tpinWithIL.subgraph(vpred = (vid, attr) => !attr.ishuman).
            mapEdges(map = { edge =>
                val eattr = InfluEdgeAttr()
                eattr.jy_bl = edge.attr.w_trade
                eattr.kg_bl = edge.attr.w_gd
                eattr.tz_bl = edge.attr.w_tz
                eattr.il_bl = edge.attr.w_IL
                eattr
            }).
            mapVertices(map= {(vid,vattr)=>
                val newattr = InfluVertexAttr(vattr.nsrsbh,vattr.name)
                newattr.xydj = vattr.xydj
                newattr.xyfz = vattr.xyfz
                newattr.wtbz = vattr.wtbz
                newattr
            })
        toReturn
    }

    type Influence = Seq[(VertexId, Double, Double, Double)]
    type Path = Seq[(VertexId,Double)]
    type Paths = Seq[Seq[(VertexId,Double)]]

    //annotation of david:默认要求重叠度为2（严格版）
    def findFrequence(d: Seq[(VertexId, Seq[(Long,Double)])], degree: Int = 2): Seq[((Long, Long),Double)] = {
        val frequencies = d
        val result =
            for (i <- 0 until frequencies.length) yield
                for (j <- i + 1 until frequencies.length) yield {
                    val (vid1, list1) = frequencies(i)
                    val (vid2, list2) = frequencies(j)
                    val map1 = list1.toMap
                    val map2 = list2.toMap
                    val intersect = map1.keySet.intersect(map2.keySet)
                    var weight =0D
//                    if(intersect.size>5)
                    for(key <- intersect)
                        weight+= map1.get(key).get.min(map2.get(key).get)
                    if (weight>0)
                        Option(Iterable(((vid1, vid2),weight), ((vid2, vid1),weight)))
                    else
                        Option.empty
                }
        result.flatten.filter(!_.isEmpty).map(_.get).flatten
    }


    //annotation of david:direction=1 表示从源点向终点传递消息 =-1表示反向传递消息
    def getPath(graph: Graph[Paths, WholeEdgeAttr], weight: Double, maxIteratons: Int = Int.MaxValue, initLength: Int = 1, direction: Int = 1, justGD: Boolean = false) = {
        // 发送路径
        def sendPaths(edge: EdgeContext[Paths, Double, Paths],
                      length: Int): Unit = {
            // 过滤掉仅含交易权重的边 以及非起点
            val satisfied = edge.srcAttr.filter(_.size == length).filter(!_.contains(edge.dstId))
            if (satisfied.size > 0) {
                // 向终点发送顶点路径集合
                edge.sendToDst(satisfied.map(_ ++ Seq((edge.dstId,edge.attr))))
            }
        }
        def sendPathsReverse(edge: EdgeContext[Paths, Double, Paths],
                             length: Int): Unit = {
            // 过滤掉仅含交易权重的边 以及非起点
            val satisfied = edge.dstAttr.filter(_.size == length).filter(!_.contains(edge.srcId))
            if (satisfied.size > 0) {
                // 向终点发送顶点路径集合
                edge.sendToSrc(satisfied.map(Seq((edge.srcId,edge.attr)) ++ _))
            }
        }
        // 路径长度
        val degreesRDD = graph.degrees.cache()
        // 使用度大于0的顶点和边构建图
        var preproccessedGraph = graph
            .subgraph(epred = triplet =>
                triplet.attr.isAntecedent(weight, justGD)
            )
            .outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0)))
            .subgraph(vpred = {
                case (vid, (vattr, degreesVar)) =>
                    degreesVar > 0
            }
            )
            .mapVertices { case (vid, (list, degreesVar)) => list }
            .mapEdges(edge =>Seq(edge.attr.w_control,edge.attr.w_gd,edge.attr.w_tz).max)
            .cache()
        var i = initLength
        var messages: VertexRDD[Paths] = null

        //annotation of david:messages长度为i+1
        if (direction == 1)
            messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _)
        else
            messages = preproccessedGraph.aggregateMessages[Paths](sendPathsReverse(_, i), _ ++ _)
        var activeMessages = messages.count()
        var prevG: Graph[Paths, Double] = null
        while (activeMessages > 0 && i <= maxIteratons) {
            prevG = preproccessedGraph

            //annotation of david:长度=maxIteratons+1
            preproccessedGraph = preproccessedGraph.joinVertices[Paths](messages)((id, vd, path) => vd ++ path).cache()
            print("iterator " + i + " finished! ")
            i += 1
            val oldMessages = messages
            if (direction == 1)
                messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _).cache()
            else
                messages = preproccessedGraph.aggregateMessages[Paths](sendPathsReverse(_, i), _ ++ _).cache()
            activeMessages = messages.count()
            oldMessages.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            prevG.edges.unpersist(blocking = false)
        }
        //         printGraph[Paths,Int](preproccessedGraph)
        preproccessedGraph.vertices
    }

    //annotation of david:添加董事会互锁边,默认重叠度要求为2
    def addIL(graph: Graph[WholeVertexAttr, WholeEdgeAttr], weight: Double, degree: Int = 2) = {
        //annotation of david:仅从单条路径发送消息，造成了算法的不一致
        // 含义：每个人所控制及间接控制企业的列表
        val initialGraph = graph
            .mapVertices { case (id, vattr) =>
                if (vattr.ishuman) Seq(Seq((id,1D))) else Seq[Seq[(VertexId,Double)]]()
            }

        //annotation of david:此处无法使用反向获取路径，因为要求源点必须是人

        //annotation of david:存在平行路径的问题
        val messagesOfControls = getPath(initialGraph, weight, maxIteratons = 3).mapValues(lists => lists.filter(_.size > 1).
            map{ case list=> val influ = list.map(_._2).min
                (list.head._1,influ)
            }).
            filter(_._2.size > 0)
        //        val messagesOfCompanys = getPath(initialGraph,weight,maxIteratons = 3).mapValues(lists=>lists.filter(_.size>1).map(_.last))
        //     println("messagesOfCompanys:")
        //     messagesOfCompanys.collect().foreach(println)
        val messagesOffDirection = messagesOfControls
            .flatMap { case (vid, controllerList) =>
                controllerList.map(controller =>
                    (controller._1, (vid, controllerList))
                )
            }.groupByKey().map { case (vid, ite) => (vid, ite.toSeq) }
        //     println("messagesOffDirection:")
        //     messagesOffDirection.collect().foreach(println)
        val newILEdges = messagesOffDirection
            .flatMap { case (dstid, list) => findFrequence(list, degree) }
            .distinct
            .map { case ((src,dst), weight) =>
                val edgeAttr = WholeEdgeAttr()
                edgeAttr.is_IL = true
                edgeAttr.w_IL = weight
                Edge(src, dst, edgeAttr)
            }
        val newEdges = graph.edges.union(newILEdges).
            map(e=>((e.srcId,e.dstId),e.attr)).
            reduceByKey(WholeEdgeAttr.combine).
            map(e=>Edge(e._1._1,e._1._2,e._2))
        Graph(graph.vertices, newEdges)
    }

}

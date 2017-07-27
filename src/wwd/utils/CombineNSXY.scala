package wwd.utils

import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import wwd.entity.{InfluenceEdgeAttr, VertexAttr, EdgeAttr}
import scala.collection.Seq

/**
  * Created by weiwenda on 2017/3/20.
  */
object CombineNSXY {

    //    def AggregateMessage(xyfz: Int, listMessage: scala.Seq[( Int, Double)]): Int = {
    //        val totalWeight = listMessage.filter(_._1<xyfz).map(_._2).sum
    //        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
    //        var i = 0
    //        var res = 0D
    //        while(i< Sortedlist.size){
    //            val (cur_fx,weight) = Sortedlist(i)
    //            if(cur_fx < xyfz){
    //                res += (xyfz-cur_fx) *weight/totalWeight
    //            }
    //            i+=1
    //        }
    //        (xyfz - res).toInt
    //    }
    def AggregateMessage(xyfz: (Int, Boolean), listMessage: scala.Seq[(Int, Double)],alpha:Double): Int = {
//        val totalWeight = listMessage.map(_._2).sum
//        val alpha_fix = alpha+0.001
//        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
//        var i = 0
//        var res = 0D
//        while (i < Sortedlist.size) {
//            val (cur_fx, weight) = Sortedlist(i)
//            res += (xyfz._1 - cur_fx) * weight / totalWeight
//            i += 1
//        }
//        (xyfz._1 - res).toInt

//        if(( xyfz._1 - (1-alpha_fix)/alpha_fix * res).toInt <= 100) ( alpha * xyfz._1 - (1-alpha_fix)/alpha_fix * res).toInt  else 100

        val totalWeight = listMessage.map(_._2).sum
        var before = 0D
        listMessage.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
        val result = alpha * xyfz._1/10 + (1-alpha) * before
        result.toInt
    }


    def AggregateMessage(listMessage: Seq[(Int, Double)]): Int = {
        val totalWeight = listMessage.map(_._2).sum
        var res = 0D
        listMessage.foreach { case (cur_fx, weight) => res += cur_fx * weight / totalWeight }
        res.toInt
    }
    def AggregateMessage(xyfz: (PartitionID, Boolean), listMessage: (scala.Seq[(PartitionID, Double)], PartitionID, PartitionID),alpha:Double) = {
        val totalWeight = listMessage._1.map(_._2).sum
        var before = 0D
        listMessage._1.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
        val result = alpha * before + (1-alpha) * listMessage._2/listMessage._3
        result
    }
    //annotation of david:先对已有评分的节点进行修正，（只拉低）
    def run(influenceGraph: Graph[(Int, Boolean), Double],alpha:Double=0.5): Graph[(Int, Int, Boolean), Double] = {
        val fzMessage = influenceGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
            if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0  ) {//&& ctx.srcAttr._1 < 90 && ctx.dstAttr._1 < 90
                val weight = ctx.attr //* (100 - ctx.srcAttr._1) / 100D
                ctx.sendToDst(Seq((ctx.srcAttr._1, weight)))
            }, _ ++ _).cache()

        val fixAlreadyGraph = influenceGraph.outerJoinVertices(fzMessage) {
            case (vid, vattr, listMessage) =>
                if (listMessage.isEmpty)
                    (vattr._1, vattr._1, vattr._2)
                else {
                    (vattr._1, AggregateMessage(vattr, listMessage.get,alpha), vattr._2)
                }
        }.cache()
        val fzMessage2 = fixAlreadyGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
            if (ctx.dstAttr._1 == 0 && ctx.srcAttr._1 > 0 ) {//(ctx.dstAttr._1 == 0|| ctx.dstAttr._1 >90 ) && ctx.srcAttr._1 > 0 && ctx.srcAttr._1 < 90
                //annotation of david:分数越低的企业影响力越大
                val weight = ctx.attr * (100 - ctx.srcAttr._2) * (100 - ctx.srcAttr._2)
                ctx.sendToDst(Seq((ctx.srcAttr._2, weight)))
            }, _ ++ _).cache()
        val fixNotyetGraph = fixAlreadyGraph.outerJoinVertices(fzMessage2) {
            case (vid, vattr, listMessage) =>
                if (listMessage.isEmpty)
                    vattr
                else {
                    (vattr._1, AggregateMessage(listMessage.get), vattr._3)
                }
        }.cache()

        fzMessage.unpersist(blocking = false)
        fzMessage2.unpersist(blocking = false)
        fixAlreadyGraph.unpersistVertices(blocking = false)
        fixAlreadyGraph.edges.unpersist(blocking = false)

        val max = fixNotyetGraph.vertices.map(_._2._2).max()
        println(max)
        fixNotyetGraph.mapVertices{case (vid,(old,newfz,wtbz)) =>(old,(newfz/max.toDouble*100).toInt,wtbz)}
    }



    //annotation of david:考虑出度、入度、影响值、原始评分
    def runPeerTrust(influenceGraph:Graph[(Int, Boolean), Double],alpha:Double=0.5): Graph[(Int, Int, Boolean), Double] = {
        val outdegree = influenceGraph.outDegrees
        val indegree = influenceGraph.inDegrees
        val oldfz = influenceGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
            if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0) {
                val weight = ctx.attr
                ctx.sendToDst(Seq((ctx.srcAttr._1, weight)))
            }, _ ++ _).cache()
        val message = oldfz.leftOuterJoin(outdegree).leftOuterJoin(indegree).map{
            case (vid,(((oldfz,outd),ind))) => (vid,(oldfz,outd.getOrElse(1),ind.getOrElse(1)))
        }

        val fixAlreadyGraph = influenceGraph.outerJoinVertices(message) {
            case (vid, vattr, listMessage) =>
                if (listMessage.isEmpty)
                    (vattr._1, vattr._1.toDouble, vattr._2)
                else {
                    (vattr._1, AggregateMessage(vattr, listMessage.get,alpha), vattr._2)
                }
        }.cache()
        val max = fixAlreadyGraph.vertices.map(_._2._2).max()
        println(max)
        fixAlreadyGraph.mapVertices{case (vid,(old,newfz,wtbz)) =>(old,(newfz/max*100).toInt,wtbz)}
    }

    //annotation of david: 点 原始评分 边 影响值 输出：点 原始评分 新评分 影响值
    def runPageRank(graph: Graph[(Int, Boolean), Double], resetProb: Double = 0.15, tol: Double = 1): Graph[(Int, Int, Boolean), Double] = {
        val degree = graph.aggregateMessages[Seq[Double]](ctx => {
            ctx.sendToSrc(Seq(ctx.attr))
        }, _ ++ _).map(e=> (e._1,e._2.sum))
        // Initialize the pagerankGraph with each edge attribute
        // having weight 1/outDegree and each vertex with attribute 1.0.
        val pagerankGraph = graph
            .outerJoinVertices(degree) {
                case (vid, old, degree) =>if(degree.isEmpty) (old._1,1D) else (old._1, degree.get)
            }
            .mapTriplets(e => e.attr / e.srcAttr._2)
            // Set the vertex attributes to (initalPR, delta = 0)
            .mapVertices { case (id, (old, degree)) => (old.toDouble, 0.0) }
            .cache()

        // Define the three functions needed to implement PageRank in the GraphX
        // version of Pregel
        def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
            val (oldPR, lastDelta) = attr
            if(msgSum== -1D)
                (oldPR,tol+1)
            else if(msgSum>100){
                val newPR = oldPR * resetProb + (1.0 - resetProb) * 100
                (newPR, newPR - oldPR)
            }else{
                val newPR = oldPR * resetProb + (1.0 - resetProb) * msgSum
                (newPR, newPR - oldPR)
            }
        }

        def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
            if (edge.srcAttr._2 > tol) {
                Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
            } else {
                Iterator.empty
            }
        }

        def messageCombiner(a: Double, b: Double): Double = a + b

        // The initial message received by all vertices in PageRank
        val initialMessage = -1D // resetProb / (1.0 - resetProb)

        // Execute a dynamic version of Pregel.
        val vp =
            (id: VertexId, attr: (Double, Double), msgSum: Double) =>
                vertexProgram(id, attr, msgSum)

        val result = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out, maxIterations = 4)(
            vp, sendMessage, messageCombiner)
            .mapVertices((vid, attr) => attr._1)
        val max = result.vertices.map(_._2).max()
        println(max)
        result.mapVertices{case (vid,fz) => fz/max*100}.
            outerJoinVertices(graph.vertices) { case (vid, newf, opt) => (opt.get._1, newf.toInt, opt.get._2) }
    } // end of deltaPageRank
}

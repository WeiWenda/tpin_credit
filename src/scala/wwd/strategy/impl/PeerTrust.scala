package wwd.strategy.impl

import org.apache.spark.graphx.{Graph, PartitionID}

import scala.collection.Seq

class PeerTrust extends credit_Fuzz{
  override var message3:String = "PreeTrust"
  //annotation of david:考虑出度、入度、影响值、原始评分
  override def computeCreditScore(influenceGraph: Graph[(Int, Boolean), Double]): Graph[ResultVertexAttr, ResultEdgeAttr] = {
    val outdegree = influenceGraph.outDegrees
    val indegree = influenceGraph.inDegrees
    val oldfz = influenceGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
      if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0) {
        val weight = ctx.attr
        ctx.sendToDst(Seq((ctx.srcAttr._1, weight)))
      }, _ ++ _).cache()
    val message = oldfz.leftOuterJoin(outdegree).leftOuterJoin(indegree).map {
      case (vid, (((oldfz, outd), ind))) => (vid, (oldfz, outd.getOrElse(1), ind.getOrElse(1)))
    }

    val fixAlreadyGraph = influenceGraph.outerJoinVertices(message) {
      case (vid, vattr, listMessage) =>
        if (listMessage.isEmpty)
          (vattr._1, vattr._1.toDouble, vattr._2)
        else {
          (vattr._1, AggregateMessage(vattr, listMessage.get, alpha), vattr._2)
        }
    }.cache()
    val max = fixAlreadyGraph.vertices.map(_._2._2).max()
    println(max)
    fixAlreadyGraph.mapVertices { case (vid, (old, newfz, wtbz)) => ResultVertexAttr(old, (newfz / max.toDouble * 100).toInt, wtbz) }
      .mapEdges(e => ResultEdgeAttr(e.attr))
  }
  def AggregateMessage(xyfz: (PartitionID, Boolean), listMessage: (scala.Seq[(PartitionID, Double)], PartitionID, PartitionID), alpha: Double) = {
    val totalWeight = listMessage._1.map(_._2).sum
    var before = 0D
    listMessage._1.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
    val result = alpha * before + (1 - alpha) * listMessage._2 / listMessage._3
    result
  }

}

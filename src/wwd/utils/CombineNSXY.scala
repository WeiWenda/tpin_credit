package wwd.utils
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import wwd.entity.{ InfluenceEdgeAttr, VertexAttr, EdgeAttr}
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
    def AggregateMessage(xyfz: Int, listMessage: scala.Seq[( Int, Double)]): Int = {
        val totalWeight = listMessage.map(_._2).sum
        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
        var i = 0
        var res = 0D
        while(i< Sortedlist.size){
            val (cur_fx,weight) = Sortedlist(i)
            res += (xyfz-cur_fx) *weight/totalWeight
            i+=1
        }
        (xyfz - res).toInt
    }


    def AggregateMessage(listMessage: Seq[(Int, Double)]): Int = {
        val totalWeight = listMessage.map(_._2).sum
        var res = 0D
        listMessage.foreach{ case (cur_fx,weight)=> res+= cur_fx *weight/totalWeight }
        res.toInt
    }

    //annotation of david:先对已有评分的节点进行修正，（只拉低）
    def run(influenceGraph: Graph[Int, Double]):  Graph[(Int,Int), Double] ={
        val fzMessage = influenceGraph.aggregateMessages[Seq[(Int,Double)]](ctx =>
            if(ctx.srcAttr > 0 && ctx.dstAttr > 0){
                val weight = ctx.attr * (100-ctx.srcAttr)/100D
                ctx.sendToDst(Seq((ctx.srcAttr,weight)))
            },_++_).cache()

        val fixAlreadyGraph = influenceGraph.outerJoinVertices(fzMessage){
            case(vid,vattr,listMessage) =>
                if (listMessage.isEmpty)
                    (vattr,vattr)
                else{
                    (vattr, AggregateMessage(vattr,listMessage.get))
                }
        }.cache()
        val fzMessage2 = fixAlreadyGraph.aggregateMessages[Seq[(Int,Double)]](ctx =>
            if(ctx.dstAttr._1 == 0 && ctx.srcAttr._1 > 0) {
                //annotation of david:分数越低的企业影响力越大
                val weight = ctx.attr * (100-ctx.srcAttr._2)*(100-ctx.srcAttr._2)
                ctx.sendToDst(Seq((ctx.srcAttr._2, weight)))
            },_++_).cache()
        val fixNotyetGraph = fixAlreadyGraph.outerJoinVertices(fzMessage2){
            case(vid,vattr,listMessage) =>
                if (listMessage.isEmpty)
                    vattr
                else{
                    (vattr._1,AggregateMessage(listMessage.get))
                }
        }.cache()
        fzMessage.unpersist(blocking = false)
        fzMessage2.unpersist(blocking = false)
        fixAlreadyGraph.unpersistVertices(blocking = false)
        fixAlreadyGraph.edges.unpersist(blocking = false)
        fixNotyetGraph
    }
}

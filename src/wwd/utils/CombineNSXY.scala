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

    def AggregateMessage(xyfz: Int, listMessage: scala.Seq[( Int, Double)]): Int = {
        val totalWeight = listMessage.map(_._2).sum
        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
        var i = 0
        var res = 0D
        while(i< Sortedlist.size){
            val (cur_fx,weight) = Sortedlist(i)
            if(cur_fx < xyfz){
                res += (cur_fx-xyfz) *weight/totalWeight
            }
            i+=1
        }
//        if((xyfz + res).toInt > 0)
//            (xyfz + res).toInt
//        else
//            xyfz
        (xyfz + res).toInt
    }

    def AggregateMessage(listMessage: Seq[(Int, Double)]): Int = {
        val totalWeight = listMessage.map(_._2).sum
        var res = 0D
        listMessage.foreach{ case (cur_fx,weight)=> res+= cur_fx *weight/totalWeight }
        res.toInt
    }

    //annotation of david:先对已有评分的节点进行修正，（只拉低）
    def run(influenceGraph: Graph[Int, Double]):  Graph[Int, Double] ={
        val fzMessage = influenceGraph.aggregateMessages[Seq[(Int,Double)]](ctx =>
            if(ctx.srcAttr > 0 && ctx.dstAttr > 0){
                ctx.sendToDst(Seq((ctx.srcAttr,ctx.attr)))
            },_++_).cache()

        val fixAlreadyGraph = influenceGraph.outerJoinVertices(fzMessage){
            case(vid,vattr,listMessage) =>
                if (listMessage.isEmpty)
                    vattr
                else{
                    AggregateMessage(vattr,listMessage.get)
                }
        }.cache()
        val fzMessage2 = fixAlreadyGraph.aggregateMessages[Seq[(Int,Double)]](ctx =>
            if(ctx.dstAttr == 0 ) {
                ctx.sendToDst(Seq((ctx.srcAttr, ctx.attr)))
            },_++_).cache()
        val fixNotyetGraph = fixAlreadyGraph.outerJoinVertices(fzMessage2){
            case(vid,vattr,listMessage) =>
                if (listMessage.isEmpty)
                    vattr
                else{
                    AggregateMessage(listMessage.get)
                }
        }.cache()
        fzMessage.unpersist(blocking = false)
        fzMessage2.unpersist(blocking = false)
        fixAlreadyGraph.unpersistVertices(blocking = false)
        fixAlreadyGraph.edges.unpersist(blocking = false)
        fixNotyetGraph
    }
}

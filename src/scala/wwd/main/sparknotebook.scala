package wwd.main

import org.apache.spark.graphx._
import org.apache.spark.sql.catalyst.util.StringUtils
import wwd.entity.{EdgeAttr, VertexAttr}
import wwd.entity.impl.{WholeEdgeAttr, WholeVertexAttr}
import wwd.strategy.impl.credit_DS
import wwd.utils.{OracleTools, HdfsTools, Parameters}
import org.apache.commons.lang3

/**
  * Created by weiwenda on 2018/3/5.
  */
object sparknotebook {
  /**
   *Author:weiwenda
   *Description:idea push test
   *Date:11:48 2018/3/7
   */
  def main(args: Array[String]) {
    val method =new credit_DS()
    val graph = method.getGraph(method.sc,method.session)
    val hdfsDir:String = Parameters.Dir
    val paths = Seq(s"${hdfsDir}/init_vertices", s"${hdfsDir}/init_edges")
    val initGraph=HdfsTools.getFromObjectFile[WholeVertexAttr, WholeEdgeAttr](method.sc, paths(0),paths(1)).persist()
    initGraph.vertices.filter(_._2.ishuman).count
    initGraph.triplets.filter(e=>e.srcAttr.ishuman&& !e.dstAttr.ishuman).map(_.dstId).distinct().count
    graph.vertices.count
    graph.degrees.count
    val edges = initGraph.triplets.filter(e=>e.srcAttr.ishuman&& !e.dstAttr.ishuman)
    println(edges.filter(_.attr.w_gd>0).count)
    println(edges.filter(_.attr.w_tz>0).count)
    val edges2 = initGraph.triplets.filter(e=> !e.srcAttr.ishuman&& !e.dstAttr.ishuman)
    println(edges2.filter(_.attr.w_gd>0).count)
    println(edges2.filter(_.attr.w_tz>0).count)
    println(edges.filter(_.attr.w_control>0).count)
    println(edges.filter(_.attr.w_trade>0).count)
    println(graph.edges.filter(_.attr.il_bl>0).count)
    println(graph.edges.count)
    val degrees = graph.degrees.persist
    val ALL_VERTEX_TMP = graph.vertices.join(degrees).map(vertex => (vertex._2._2,1))
      .reduceByKey(_+_).sortByKey().collect
    ALL_VERTEX_TMP.filter(_._1>=10).map(_._2).sum
    val degreesRDD = graph.degrees.cache()
    var preproccessedGraph = graph
      .outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0)))
      .subgraph(vpred = {
        case (vid, (vattr, degreesVar)) =>
          degreesVar > 0
      }
      )
    val newTpin = preproccessedGraph.mapVertices((vid,attr)=>vid).mapEdges(attr=>"none")
    // 连通图划分社团
    val communityIds = newTpin.connectedComponents.vertices.persist()
    communityIds.map(e=>(e._2,1)).reduceByKey(_+_).
      map(e=>(e._2,1)).reduceByKey(_+_).sortByKey().collect()

    val triCountGraph = preproccessedGraph.triangleCount()
    val maxTrisGraph = degreesRDD.mapValues(d=>d*(d-1)/2.0)
    val clusterCoef = triCountGraph.vertices.innerJoin(maxTrisGraph){
      case(vertexId,triCount,maxTris)=>
        if(maxTris ==0) 0 else triCount/maxTris
    }
    println(clusterCoef.map(_._2).sum/preproccessedGraph.vertices.count)
    //annotation of david:首先滤除带有字符的纳税人电子档案号，然后去重，最后输出到oracle
    val rdd =  preproccessedGraph.vertices.
      filter{case(vid,(attr,degree))=> lang3.StringUtils.isNumeric(attr.nsrdzdah)}.
      map{case(vid,(attr,degree))=>(BigDecimal(attr.nsrdzdah).longValue(),vid)}.
      reduceByKey((a,b)=>a.min(b)).
      map{case(nsrdzdah,vid)=>OracleTools.Vertex2DAH(vid,nsrdzdah)}
    OracleTools.saveVertexs(rdd,method.session)
  }

}

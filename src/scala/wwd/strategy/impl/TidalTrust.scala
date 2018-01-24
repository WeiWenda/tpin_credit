package wwd.strategy.impl

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import wwd.utils.HdfsTools

import scala.collection.Seq

class TidalTrust extends credit_DS{
  lazy override val message2:String = "TidalTrust"
  //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
  override def computeInfluence(tpin: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
    // tpin size: vertices:93523 edges:633300
    val belAndPl = tpin.mapTriplets { case triplet =>
      //            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
      //annotation of david:bel为概率下限，pl为概率上限
      val bel = _computeBel(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl, method)
      val pl = _computePl(triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl, method)
      val attr = DSEdgeAttr(bel, pl, triplet.srcAttr.nsrdzdah, triplet.dstAttr.nsrdzdah, triplet.attr)
      attr
    }
    val simplifiedGraph = credit_DS.selectNeighbor[String](belAndPl.mapVertices((vid, vattr) => vattr.nsrdzdah))
    //annotation of david:企业对自身的bel和pl均为1
    val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, nsrdzdah, 1.0, 1.0, InfluEdgeAttr()))) }
    //initGraph size: vertices:93523 edges:132965
    val paths = credit_DS.getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)
    //        paths.saveAsObjectFile("/tpin/wwd/influence/paths")
    //        sc.objectFile[(VertexId,MessagePropagation.Paths)](verticesFilePath)
    //paths:93523

    //annotation of david:使用第一种三角范式
    val influenceEdge = influenceOnPathTidalTrust(paths, lambda, session)
    val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

    //annotation of david:滤除影响力过小的边
    val finalInfluenceGraph = credit_DS.influenceInTotal(influenceGraph)
    finalInfluenceGraph
    //finalInfluenceGraph size: vertices:93523 edges:1850050
  }
  //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
  def influenceOnPathTidalTrust[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double, InfluEdgeAttr)]]](paths: RDD[(VertexId, T)], lambda: Int, sqlContext: SparkSession) = {
    val influences = paths.map { case (vid, vattr) =>
      val DAG = credit_DS.graphReduce(vattr)
      val influenceSinglePath = DAG.map { path =>
        val res = path.reduceLeft((a, b) => credit_DS.combineInfluence(a, b, lambda))
        //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
        (res._1, res._3)
      }
      (vid, influenceSinglePath)
    }
      .flatMap { case (vid, list) =>
        list.map { case (dstid, pTrust) => ((vid, dstid), pTrust) }
      }.aggregateByKey((0D))(combinePath1, combinePath2).
      map { case ((vid, dstid), pTrust) => Edge(vid, dstid, pTrust) }
    influences
  }
  def combinePath1(x: Double, y: Double) = {
    if (x > y) x else y
  }

  def combinePath2(x: Double, y: Double) = {
    if (x > y) x else y
  }


}

package wwd.strategy.impl

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import wwd.evaluation.{EvaluateResult, Measurement}

import scala.collection.Seq
/**
* Author: weiwenda
* Description: 继承自credit_DS，重写了computeInfluence，采用模糊推理方法计算影响值
* Date: 下午9:00 2017/11/29
*/
class credit_Fuzz(ignoreIL:Boolean=false) extends credit_DS {
  override val message1:String = s"模糊推理"
  override def computeInfluence(tpin: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
    // tpin size: vertices:93523 edges:633300
    val belAndPl = tpin.mapTriplets { case triplet =>
      //            val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
      //annotation of david:bel为概率下限，pl为概率上限
      //                      当ignoreIL为true时，无视il_bl
      val bel = computeFuzzScore(if(ignoreIL) 0 else triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
      val attr = DSEdgeAttr(bel, bel, triplet.srcAttr.nsrdzdah, triplet.dstAttr.nsrdzdah, triplet.attr)
      attr
    }
    val simplifiedGraph = credit_DS.selectNeighbor[String](belAndPl.mapVertices((vid, vattr) => vattr.nsrdzdah))
    //annotation of david:企业对自身的bel和pl均为1
    val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, nsrdzdah, 1.0, 1.0, InfluEdgeAttr()))) }
    //initGraph size: vertices:93523 edges:132965
    val paths = credit_DS.getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)

    //annotation of david:使用第一种三角范式
    val influenceEdge = _influenceOnPath(paths, lambda, session, bypass)
    val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

    //annotation of david:滤除影响力过小的边
    val finalInfluenceGraph = credit_DS.influenceInTotal(influenceGraph)
    finalInfluenceGraph
    //finalInfluenceGraph size: vertices:93523 edges:1850050
  }
  def computeMembership(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double, point: Double): Double = {
    //annotation of david:四条规则取最大
    Seq(rule("il", il_bl, point), rule("tz", tz_bl, point), rule("tz", kg_bl, point), rule("jy", jy_bl, point)).max
  }

  def computeFuzzScore(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
    val score: Array[Double] = new Array[Double](11)
    //annotation of david:采样
    for (i <- Range(0, 11)) {
      score(i) = computeMembership(il_bl, tz_bl, kg_bl, jy_bl, i / 10D)
    }
    //annotation of david:反模糊化 :最大平均去模糊化 VS 重心面积中心去模糊 VS 最大隶属度法
    val result = score.zipWithIndex.sortWith((a, b) => if (a._1 == b._1) (b._2 - a._2) > 0 else (a._1 - b._1) > 0)(0)._2 / 10D
    //        val result = score.sum/score.size
    //        val result = score.zipWithIndex.aggregate(0D)({case (cur,(score,index))=>
    //            cur +score * index/10D
    //        },_+_)/score.sum
    result
  }
  //annotation of david:模糊逻辑
  def rule(s: String, bl: Double, point: Double) = {
    var toReturn = 0D
    //        val norm = if(point>=0.8) 1 else if(point <= 0.2 ) 0 else  1/0.6 * point - 0.334
    val norm = if (point >= 0.8) 1 else 1.25 * point
    s match {
      case "il" =>
        if (bl >= 0.1)
          toReturn = norm
        else
          toReturn = if (norm < 10 * bl) norm else 10 * bl
      case "tz" =>
        if (bl >= 0.2)
          toReturn = norm
        else
          toReturn = if (norm < 5 * bl) norm else 5 * bl
      case "jy" =>
        if (bl >= 0.1)
          toReturn = norm
        else
          toReturn = if (norm < 10 * bl) norm else 10 * bl
    }
    toReturn
  }

}

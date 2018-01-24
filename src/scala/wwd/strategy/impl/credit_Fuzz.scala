package wwd.strategy.impl

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, EdgeContext, VertexId, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import wwd.entity.EdgeAttr
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import wwd.utils.{HdfsTools, OracleTools}

import scala.collection.{ Seq}
import scala.collection.mutable.HashMap

/**
* Author: weiwenda
* Description: 继承自credit_DS，重写了computeInfluence，采用模糊推理方法计算影响值
* Date: 下午9:00 2017/11/29
*/
case class FuzzEdgeAttr(val influ: Double) extends EdgeAttr
class credit_Fuzz(ignoreIL:Boolean=false,forceReAdjust:Boolean=false,
                  var forceReComputePath:Boolean=false
                 ) extends credit_DS(forceReAdjust=forceReAdjust) {
  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]
  override lazy val message1:String = s"模糊推理"
  var rules:HashMap[String,Map[String,Double]] = _

  def _getMap(seq1: Array[(Double, Int)],amount: Long): Map[String,Double] ={
    Range.Double(0,1.01,0.02).map(e=>(e,seq1.filter(_._1<=e).map(_._2).sum/amount.toDouble)).toMap.
      map{case(key,value)=>
        (key.formatted("%.3f"),value)
      }
  }

  def _getRules(graph: Graph[InfluVertexAttr, InfluEdgeAttr])= {
    val result = HashMap[String,Map[String,Double]]()
    var seq1 = Array[(Double, Int)]()
    var amount:Long = 0
    seq1 = graph.edges.filter(e=>e.attr.il_bl>0).map(e=>(Math.ceil(e.attr.il_bl/0.02)*0.02,1)).reduceByKey(_+_).sortByKey().collect()
    amount = graph.edges.filter(e=>e.attr.il_bl>0).count()
    result.put("il",_getMap(seq1,amount))
    seq1 = graph.edges.filter(e=>e.attr.tz_bl>0).map(e=>(Math.ceil(e.attr.tz_bl/0.02)*0.02,1)).reduceByKey(_+_).sortByKey().collect()
    amount = graph.edges.filter(e=>e.attr.tz_bl>0).count()
    result.put("tz",_getMap(seq1,amount))
    seq1 = graph.edges.filter(e=>e.attr.kg_bl>0).map(e=>(Math.ceil(e.attr.kg_bl/0.02)*0.02,1)).reduceByKey(_+_).sortByKey().collect()
    amount = graph.edges.filter(e=>e.attr.kg_bl>0).count()
    result.put("kg",_getMap(seq1,amount))
    seq1 = graph.edges.filter(e=>e.attr.jy_bl>0).map(e=>(Math.ceil(e.attr.jy_bl/0.02)*0.02,1)).reduceByKey(_+_).sortByKey().collect()
    amount = graph.edges.filter(e=>e.attr.jy_bl>0).count()
    result.put("jy",_getMap(seq1,amount))
    result
  }
  def _newRule(s: String, bl: Double) = {
    rules.get(s).get.get((Math.ceil(bl/0.02)*0.02).min(1D).formatted("%.3f")).get
  }
  def _computeFuzzScore(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
    //annotation of david:计算最大的规则前件隶属度
    val upper = Seq(_newRule("il", il_bl), _newRule("tz", tz_bl), _newRule("kg", kg_bl), _newRule("jy", jy_bl)).max
    upper
  }

  def _getOrComputeInflu(tpin: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
    val paths = Seq(s"${hdfsDir}/fuzz_vertices", s"${hdfsDir}/fuzz_edges")
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc,paths(0)) || !HdfsTools.Exist(sc,paths(1)) || forceReComputePath) {
      rules = _getRules(tpin)
      val inferenced = tpin.mapTriplets { case triplet =>
        //annotation of david:bel为概率下限，pl为概率上限
        //                      当ignoreIL为true时，无视il_bl
        val bel = _computeFuzzScore(if(ignoreIL) 0 else triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
        val attr = FuzzEdgeAttr(bel)
        attr
      }
      //annotation of david:此处限制了出度
      val simplifiedGraph = credit_DS.simpleGraph(inferenced,(vid_attr:(Long,FuzzEdgeAttr))=>vid_attr._2.influ,5)
      persist(simplifiedGraph,paths)
    }else{
      HdfsTools.getFromObjectFile[InfluVertexAttr, FuzzEdgeAttr](sc, paths(0), paths(1))
    }
  }

  def _getOrComputePaths(simplifiedGraph: Graph[InfluVertexAttr, FuzzEdgeAttr]) = {
    val path = s"${hdfsDir}/fuzz_path"
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc,path) || forceReComputePath) {
      //annotation of david:企业对自身的bel和pl均为1
      val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, 1.0))) }

      def sendPaths(edge: EdgeContext[Paths,FuzzEdgeAttr, Paths],
                    length: Int): Unit = {
        val satisfied = edge.dstAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.srcId))
        if (satisfied.size > 0) {
          // 向终点发送顶点路径集合，每个经过节点的id,sbh,当前经过边的bel,pl,原始4维权重
          edge.sendToSrc(satisfied.map(Seq((edge.srcId, edge.attr.influ)) ++ _))
        }
      }
      def reduceMsg(a:Paths,b:Paths):Paths=a++b
      val paths = credit_DS.getPathGeneric[Paths,FuzzEdgeAttr](initGraph,sendPaths,reduceMsg, maxIteratons = 4, initLength = 1).
        mapValues(e => e.filter(_.size > 1)).filter(e => e._2.size > 0)
      HdfsTools.checkDirExist(sc, path)
      paths.repartition(30).saveAsObjectFile(path)
    }
    sc.objectFile[(Long,Seq[Seq[(graphx.VertexId, Double)]])](path).repartition(30)
  }

  override def computeInfluence(tpin: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
    val simplifiedGraph = _getOrComputeInflu(tpin)
    val paths = _getOrComputePaths(simplifiedGraph)
    //annotation of david:使用第一种三角范式
    val influenceEdge = _influenceOnPath(paths, lambda, session)
    val influenceGraph = Graph(simplifiedGraph.vertices, influenceEdge).persist()

    //annotation of david:滤除影响力过小的边
    val finalInfluenceGraph = credit_DS.influenceInTotal(influenceGraph)
    finalInfluenceGraph
    //finalInfluenceGraph size: vertices:93523 edges:1850050
  }
  //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
  protected def _influenceOnPath(paths: RDD[(VertexId,Paths)], lambda: Int, sqlContext: SparkSession) = {
    val influences = paths.map { case (vid, vattr) =>
      val influenceSinglePath = vattr.map { path =>
        val res = path.reduceLeft((a, b) => credit_DS.combineInfluence(a, b, lambda))
        //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
        (res._1, res._2)
      }
      (vid, influenceSinglePath)
    }
      .flatMap { case (vid, list) =>
        list.map { case (dstid, influ) => ((vid, dstid),influ) }
      }.reduceByKey(_.max(_)).
      map { case ((vid, dstid), influ) => Edge(vid, dstid, influ) }
    influences
  }
  private def computeMembership(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double, point: Double): Double = {
    //annotation of david:四条规则取最大
    Seq(rule("il", il_bl, point), rule("tz", tz_bl, point), rule("tz", kg_bl, point), rule("jy", jy_bl, point)).max
  }

  private def computeFuzzScore1(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
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
  private def rule(s: String, bl: Double, point: Double) = {
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

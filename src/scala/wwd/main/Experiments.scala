package wwd.main

import java.math.BigDecimal

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import wwd.entity._
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import wwd.evaluation._
import wwd.strategy.impl._
import wwd.utils.{HdfsTools, OracleTools, Parameters}

import scala.collection.Seq
import scala.util.Random

case class OOVertexAttr(vid: Long,nsrdzdah:String, old_fz: Int,
                        nei_mean: Double, nei_wmean: Double,
                        indegree:Int,outdegree:Int,
                        nei_lnum: Int, wtbz: String)

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {

  PropertyConfigurator.configure(this.getClass.getClassLoader().getResource("mylog4j.properties"))
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  val fileLogger = LoggerFactory.getLogger("csvFile")
  val measurements = Seq(AUC(3000), RANKSCORE(), PREF(), TENDENCY())

  //annotation of david:修改wtbz based on the Neighbor situation
  def prepareGroundTruth(graph: Graph[(Int, Boolean), Double]) = {
    val msg = graph.aggregateMessages[Seq[(Int, Double)]](ctx => {
      if (ctx.dstAttr._1 > 0 && ctx.srcAttr._1 > 0) {
        ctx.sendToSrc(Seq((ctx.dstAttr._1, ctx.attr)))
        ctx.sendToDst(Seq((ctx.srcAttr._1, ctx.attr)))
      }
    }, _ ++ _)
    graph.outerJoinVertices(msg) { case (vid, (old_fz, old_wtbz), opt) =>
      var finalWtbz = old_wtbz
      if (!opt.isEmpty) {
        val list = opt.get
        val totalWeight = list.map(e => e._2).sum
        var before = 0D
        list.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
        val count = list.filter(_._1 < 70).size
        if (count > 10 && Random.nextFloat() < 0.8) finalWtbz = true
        if (before < 85 && Random.nextFloat() < 0.8) finalWtbz = true
        if (old_fz < 70 && Random.nextFloat() < 0.5) finalWtbz = true
      }
      (old_fz, finalWtbz)
    }.cache()
  }

  /**
    * Author: weiwenda
    * Description: 收集已评分企业的自身评分、邻居加权评分、邻居平均评分、邻居低分企业数量
    * Date: 下午10:51 2018/1/16
    */
  def collectNeighborInfo(fullTpin:Graph[InfluVertexAttr, Double]): RDD[OOVertexAttr] = {
    val tpin = fullTpin.mapVertices((vid, vattr) => (vattr.xyfz, vattr.wtbz))
    val fzMessage = tpin.aggregateMessages[Seq[(Int, Double)]](ctx =>
      //annotation of david:注意前提：本程序只修正已有评分的企业，因此只考虑已有评分的问题企业
      if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0) {
        ctx.sendToDst(Seq((ctx.srcAttr._1, ctx.attr)))
        ctx.sendToSrc(Seq((ctx.dstAttr._1, ctx.attr)))
      }, _ ++ _).cache()
    val nei_info = fzMessage.map { case (vid, list) =>
      val nei_lnum = list.filter(e => e._1 > 0 && e._1 < 70).size
      val totalWeight = list.size.toDouble
      val nei_mean = list.map(_._1).sum / totalWeight
      val totalWeight2 = list.map(_._2).sum
      val nei_wmean = list.map { case (cur_fx, weight) => cur_fx * weight / totalWeight2 }.sum
      (vid, (nei_lnum, nei_mean, nei_wmean))
    }
    val inDegrees = tpin.inDegrees.persist
    val outDegrees = tpin.outDegrees.persist
    fullTpin.vertices.leftOuterJoin(inDegrees).map(vertex => (vertex._1, (vertex._2._1,vertex._2._2.getOrElse(0)))).
      leftOuterJoin(outDegrees).map(vertex => (vertex._1, (vertex._2._1._1,vertex._2._1._2,vertex._2._2.getOrElse(0)))).
      join(nei_info).map { case (vid, ((influAttr,inDegree,outDegree), (nei_lnum, nei_mean, nei_wmean))) =>
      OOVertexAttr(vid=vid,nsrdzdah=influAttr.nsrdzdah, old_fz=influAttr.xyfz,
        nei_mean=nei_mean, nei_wmean= nei_wmean,
        indegree=inDegree,outdegree=outDegree,
        nei_lnum=nei_lnum, wtbz = if (influAttr.wtbz) "Y" else "N")
    }
  }

  /**
    * Author: weiwenda
    * Description: 对实验数据进行改造
    * Date: 下午4:47 2017/11/29
    */
  def applyWtbz_Random(tpin1: Graph[(Int, Boolean), Double], sc: SparkContext, sqlContext: HiveContext) = {
    val hdfsDir: String = Parameters.Dir
    val result_paths = Seq(s"${hdfsDir}/final_vertices", s"${hdfsDir}/final_edges")
    val tpin2 = Experiments.prepareGroundTruth(tpin1)
    HdfsTools.saveAsObjectFile(tpin2, sc, result_paths(0), result_paths(1))
  }

  /**
    * Author: weiwenda
    * Description: 从Oracle数据库中读取wtbz
    * Date: 下午4:48 2017/11/29
    */
  def applyNewWtbz(sc: SparkContext, sqlContext: HiveContext) = {
    val finalScore = HdfsTools.getFromObjectFile[(Int, Int, Boolean), Double](sc, "/tpin/wwd/influence/fixed_vertices", "/tpin/wwd/influence/fixed_edges")
    val dbstring = Map(
      "url" -> Parameters.DataBaseURL,
      "user" -> Parameters.DataBaseUserName,
      "password" -> Parameters.DataBaseUserPassword,
      "driver" -> Parameters.JDBCDriverString)
    val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd.
      map(row =>
        (row.getAs[BigDecimal]("VERTEXID").longValue(), row.getAs[String]("WTBZ"))).
      map(e => if (e._2.equals("Y")) (e._1, true) else (e._1, false))

    val result = finalScore.outerJoinVertices(xyjb) { case (vid, vattr, wtbzopt) =>
      (vattr._1, vattr._2, wtbzopt.getOrElse(false))
    }
    result
  }
  /**
  * Author: weiwenda
  * Description: 按照1000:1000的比例准备100批数据用做训练集
  * Date: 下午3:02 2018/1/23
  */
  class prepareTrainingDataSet(session: SparkSession,patchs:Int = 10,patchSize:Int = 1000){
    import session.implicits._
    val all = session.read.format("jdbc").
      options(OracleTools.options + (("dbtable", "WWD_DECISION_TREE"))).load().
      drop("NSRDZDAH")
    val focus_rdd = all.filter($"WTBZ"=== "Y").drop("WTBZ").rdd
    val nonfocus_rdd = all.filter($"WTBZ"=== "N").drop("WTBZ").rdd
    var patch_pointer=0

    def next() ={
      val focus = focus_rdd.takeSample(true,patchSize).map{case row=>
        LabeledPoint(1,Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble)))
      }
      val nonfocus = nonfocus_rdd.takeSample(true,patchSize).map{case row=>
        LabeledPoint(0,Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble)))
      }
      patch_pointer+=1
      session.sparkContext.parallelize(focus++nonfocus)
    }
    def hasNext()={
      patch_pointer<patchs
    }
    def last()={
      next()
    }
  }

  /**
    * Author: weiwenda
    * Description: 对比有无互锁边的区别
    * Date: 下午4:14 2017/11/29
    */
  def Experiment_3() {
    fileLogger.info("alpha,AUC,RankScore,误检率,变化趋势")
    val instance = new credit_Fuzz(ignoreIL = true)
    for (index <- Range(0, 21)) {
      log.info(s"\r开始=> alpha:${index}")
      instance.alpha = index * 0.05
      instance.run()
      fileLogger.info(s"${index},${instance.evaluation(measurements)}")
    }
    instance.persist(instance.fixedGraph)
    val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices", "/tpin/wwd/influence/fixed_edges")
    instance.persist[ResultVertexAttr, ResultEdgeAttr](instance.fixedGraph, outputPaths)
    log.info("结束")
  }

  /**
    * Author: weiwenda
    * Description: alpha值对实验效果的影响
    * Date: 下午3:12 2017/11/29
    */
  def Experiment_2() {
    fileLogger.info("alpha,AUC,RankScore,误检率,变化趋势")
    val instance = new credit_Fuzz()
    for (index <- Range(0, 21)) {
      log.info(s"\r开始=> alpha:${index}")
      instance.alpha = index * 0.05
      instance.run()
      fileLogger.info(s"${index},${instance.evaluation(measurements)}")
    }
    instance.persist(instance.fixedGraph)
    val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices", "/tpin/wwd/influence/fixed_edges")
    instance.persist[ResultVertexAttr, ResultEdgeAttr](instance.fixedGraph, outputPaths)
    log.info("结束")
  }

  /**
    * Author: weiwenda
    * Description: 比较所有5种方法的实验效果
    * Date: 下午4:22 2017/11/29
    */
  def Experiment_1() = {
    fileLogger.info("计算方法详情,AUC,RankScore,误检率,变化趋势")
    val methods = Seq(new credit_DS(), new credit_Fuzz(), new PageRank(), new PeerTrust(), new TidalTrust())
    methods.map(method => {
      log.info(s"\r开始=> ${method.description}")
      method.run()
      fileLogger.info(s"${method.description},${method.evaluation(measurements)}")
    })
    log.info("结束")
  }
}

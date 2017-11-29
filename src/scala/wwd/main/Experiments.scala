package wwd.main
import java.math.BigDecimal

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import wwd.entity._
import wwd.entity.impl.InfluVertexAttr
import wwd.evaluation._
import wwd.strategy.impl._
import wwd.utils.{HdfsTools, Parameters}

import scala.collection.Seq
import scala.util.Random

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {
  PropertyConfigurator.configure(this.getClass.getClassLoader().getResource("log4j.properties"))
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  val fileLogger = LoggerFactory.getLogger("csvFile")
  val measurements = Seq(AUC(3000),RANKSCORE(),PREF(),TENDENCY())

  //annotation of david:修改wtbz based on the Neighbor situation
  def prepareGroundTruth(graph: Graph[InfluVertexAttr, EdgeAttr]) = {
    val msg = graph.aggregateMessages[Seq[Int]](ctx => {
      if (ctx.dstAttr.xyfz > 0)
        ctx.sendToSrc(Seq((ctx.dstAttr.xyfz)))
      if (ctx.srcAttr.xyfz > 0)
        ctx.sendToDst(Seq((ctx.srcAttr.xyfz)))
    }, _ ++ _)
    graph.outerJoinVertices(msg) { case (vid, old, opt) =>
      if (!opt.isEmpty) {
        val avg = opt.get.sum / opt.get.size
        if ((old.xyfz > 0 && old.xyfz < 60) || (avg < 80 && old.xyfz > 60))
          if (Random.nextFloat() < 0.8) old.wtbz = true
        old
      } else {
        old
      }
    }.cache()
  }
  /**
  * Author: weiwenda
  * Description: 对实验数据进行改造
  * Date: 下午4:47 2017/11/29
  */
  def applyWtbz_Random(sc:SparkContext,sqlContext: HiveContext)={
    val tpin1 = HdfsTools.getFromObjectFile[InfluVertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices_init","/tpin/wwd/influence/edges_init")
    val tpin2 = Experiments.prepareGroundTruth(tpin1)
    HdfsTools.saveAsObjectFile(tpin2,sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
  }
  /**
  * Author: weiwenda
  * Description: 从Oracle数据库中读取wtbz
  * Date: 下午4:48 2017/11/29
  */
  def applyNewWtbz(sc:SparkContext,sqlContext: HiveContext)={
    val finalScore = HdfsTools.getFromObjectFile[(Int,Int,Boolean),Double](sc,"/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")
    val dbstring = Map(
      "url" -> Parameters.DataBaseURL,
      "user" -> Parameters.DataBaseUserName,
      "password" -> Parameters.DataBaseUserPassword,
      "driver" -> Parameters.JDBCDriverString)
    val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring+(("dbtable","WWD_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ","WTBZ").rdd.
      map(row =>
        (row.getAs[BigDecimal]("VERTEXID").longValue(),row.getAs[String]("WTBZ"))).
      map(e=> if(e._2.equals("Y")) (e._1,true) else (e._1,false))

    val result = finalScore.outerJoinVertices(xyjb){case (vid,vattr,wtbzopt)=>
      (vattr._1,vattr._2,wtbzopt.getOrElse(false))
    }
    result
  }
  /**
  * Author: weiwenda
  * Description: 对比有无互锁边的区别
  * Date: 下午4:14 2017/11/29
  */
  def Experiment_3(){
    fileLogger.info("alpha,AUC,RankScore,误检率,变化趋势")
    val instance = new credit_Fuzz(ignoreIL = true)
    for (index <- Range(0, 21)) {
      log.info(s"\r开始=> alpha:${index}")
      instance.alpha = index*0.05
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
  def Experiment_2(){
    fileLogger.info("alpha,AUC,RankScore,误检率,变化趋势")
    val instance = new credit_Fuzz()
    for (index <- Range(0, 21)) {
      log.info(s"\r开始=> alpha:${index}")
      instance.alpha = index*0.05
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
    val methods = Seq(new credit_DS(),new credit_Fuzz(),new PageRank(),new PeerTrust(),new TidalTrust())
    methods.map(method=>{
      log.info(s"\r开始=> ${method.description}")
      method.run()
      fileLogger.info(s"${method.description},${method.evaluation(measurements)}")
    })
    log.info("结束")
  }
}

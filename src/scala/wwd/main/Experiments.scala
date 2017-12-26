package wwd.main
import java.math.BigDecimal

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import wwd.entity._
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import wwd.evaluation._
import wwd.strategy.impl._
import wwd.utils.{HdfsTools, Parameters}

import scala.collection.Seq
import scala.util.Random

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {
//  import wwd.main.Experiments
  def main(args: Array[String]): Unit = {
    val method =new credit_Fuzz()
    val hdfsDir:String = Parameters.Dir
    val paths = Seq(s"${hdfsDir}/analyze_vertices", s"${hdfsDir}/analyze_edges")
    type SearchEdge=(Double,Int,Int)
    type SearchPaths = Seq[Seq[(Long,Int,Double)]]
    val initGraph=HdfsTools.getFromObjectFile[SearchPaths,SearchEdge](method.sc, paths(0),paths(1)).persist()
    val degrees = initGraph.outDegrees.persist
    val ALL_VERTEX_TMP = initGraph.vertices.leftOuterJoin(degrees).map(vertex => (vertex._1, (vertex._2._1,vertex._2._2.getOrElse(0))))
    val clearGraph = Graph(ALL_VERTEX_TMP, initGraph.edges).persist()
    val count = clearGraph.vertices.filter(e=>e._2._1.size>0).count
    val totalDegree =  clearGraph.vertices.filter(e=>e._2._1.size>0).map(_._2._2).sum()
    def sendPaths(edge: EdgeContext[SearchPaths,SearchEdge, SearchPaths],
                  length: Int): Unit = {
      val satisfied = edge.srcAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.dstId))
      if (satisfied.size > 0) {
        edge.sendToDst(satisfied.map( _ ++ Seq((edge.dstId,edge.attr._3,edge.attr._1))))
      }
      val satisfied2 = edge.dstAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.srcId))
      if (satisfied2.size > 0) {
        edge.sendToSrc(satisfied2.map( _ ++ Seq((edge.srcId,edge.attr._2,edge.attr._1))))
      }
    }
    val path4s=credit_DS.getPathGeneric[SearchPaths,SearchEdge](initGraph,sendPaths,(a:SearchPaths,b:SearchPaths)=>a++b,3,1)
    val a = path4s.mapValues(lists => lists.filter(path=>path.size > 1&&path.size<=4).filter(_.last._3<70)).
      filter(_._2.size > 0).
      flatMap{case(vid,paths)=>paths.map(e=>e(0)._1)}.distinct().count
    println("---------------------------=====================")
  }
  PropertyConfigurator.configure(this.getClass.getClassLoader().getResource("mylog4j.properties"))
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  val fileLogger = LoggerFactory.getLogger("csvFile")
  val measurements = Seq(AUC(3000),RANKSCORE(),PREF(),TENDENCY())

  //annotation of david:修改wtbz based on the Neighbor situation
  def prepareGroundTruth(graph: Graph[(Int, Boolean), Double]) = {
    val msg = graph.aggregateMessages[Seq[(Int,Double)]](ctx => {
      if (ctx.dstAttr._1 > 0 && ctx.srcAttr._1>0){
        ctx.sendToSrc(Seq((ctx.dstAttr._1,ctx.attr)))
        ctx.sendToDst(Seq((ctx.srcAttr._1,ctx.attr)))
      }
    }, _ ++ _)
    graph.outerJoinVertices(msg) { case (vid, (old_fz,old_wtbz), opt) =>
      var finalWtbz = old_wtbz
      if (!opt.isEmpty) {
        val list = opt.get
        val totalWeight = list.map(e => e._2).sum
        var before = 0D
        list.foreach { case (cur_fx, weight) => before += cur_fx * weight/ totalWeight }
        val count = list.filter(_._1<70).size
        if(count>10 && Random.nextFloat() < 0.8) finalWtbz = true
        if(before<85 && Random.nextFloat() < 0.8) finalWtbz = true
        if(old_fz<70 && Random.nextFloat() < 0.5 ) finalWtbz = true
      }
      (old_fz,finalWtbz)
    }.cache()
  }
  /**
  * Author: weiwenda
  * Description: 对实验数据进行改造
  * Date: 下午4:47 2017/11/29
  */
  def applyWtbz_Random(tpin1:Graph[(Int, Boolean), Double],sc:SparkContext,sqlContext: HiveContext)={
    val hdfsDir:String = Parameters.Dir
    val result_paths = Seq(s"${hdfsDir}/final_vertices", s"${hdfsDir}/final_edges")
    val tpin2 = Experiments.prepareGroundTruth(tpin1)
    HdfsTools.saveAsObjectFile(tpin2,sc,result_paths(0),result_paths(1))
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
    type SearchPaths = Seq[Seq[(Long,Int,Double)]]

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

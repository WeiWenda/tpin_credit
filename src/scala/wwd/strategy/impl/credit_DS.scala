package wwd.strategy.impl

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr, WholeEdgeAttr, WholeVertexAttr}
import wwd.entity.{EdgeAttr, VertexAttr}
import wwd.evaluation.PREF
import wwd.strategy.ALRunner
import wwd.utils.{HdfsTools, InterlockTools, OracleDBUtil, Parameters}

import scala.collection.Seq
import scala.reflect.ClassTag

case class ResultVertexAttr(old_fz: Int, new_fz: Int, wtbz: Boolean) extends VertexAttr

case class ResultEdgeAttr(influ: Double) extends EdgeAttr

case class DSEdgeAttr(val bel: Double, val pl: Double, val src: String, val dst: String, val edgeAttr: InfluEdgeAttr) extends EdgeAttr
/**
* Author: weiwenda
* Description: credit_DS继承自抽象类ALRunner，且位于继承链的最高层
*              1.实现getGraph，内置强制计算和读缓存功能
*              2.实现adjust,类似于模板方法，依次调用computeInfluence、persist和computeCreditScore三大函数
*              3.实现persist,将计算结果输出到Oracle（区别于ALRunner的重载方法persist将中间结果存储至HDFS）
*              4.实现descrioption
*
* Date: 下午8:08 2017/11/29
*/
class credit_DS(var alpha: Double = 0.5,
                var bypass: Boolean = true,
                var method: Int = 1,
                var lambda: Int = 1,
                val forceReConstruct:Boolean=false,
                val forceReAdjust:Boolean = false
               ) extends ALRunner[InfluVertexAttr, InfluEdgeAttr, ResultVertexAttr, ResultEdgeAttr](){
  SparkSubmit
  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  lazy val message1:String = s"DS powered by ${credit_DS.method1s.get(1).get}"
  lazy val message2:String = credit_DS.method2s.get(lambda).get
  lazy val message3:String = "加权偏移"
  val hdfsDir:String = Parameters.Dir
  override def description:String={
    s"${this.getClass.getSimpleName}:影响力计算方法:${message1} 传递影响计算方法:${message2} 融合方法:${message3}"
  }
  override def showDimension[VD, ED](graph: Graph[VD, ED], title: String): Unit ={
    log.info(s"\r${title}=>edges:${graph.edges.count()},vertices:${graph.vertices.count()}")
  }

  def getOrReadTpin(paths: Seq[String], forceReConstruct: Boolean) = {
    if (!HdfsTools.Exist(sc, paths(0)) || !HdfsTools.Exist(sc,paths(1)) || forceReConstruct) {
      val tpin = HdfsTools.getFromOracleTable2(session).persist()
      showDimension(tpin, "从Oracle读入")
      persist(tpin, paths)
    }else{
      HdfsTools.getFromObjectFile[WholeVertexAttr, WholeEdgeAttr](sc, paths(0),paths(1)).persist()
    }
  }
  /**
    * Author: weiwenda
    * Description: 从Oracle读入，添加互锁边，并保存至HDFS
    * Date: 下午4:22 2017/11/28
    */
  override def getGraph(sc: SparkContext, session: SparkSession) = {
    val paths = Seq(s"${hdfsDir}/addil_vertices", s"${hdfsDir}/addil_edges")
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc,paths(0)) || !HdfsTools.Exist(sc,paths(1)) || forceReConstruct) {
      val tpin = getOrReadTpin(Seq( s"${hdfsDir}/init_vertices", s"${hdfsDir}/init_edges"),forceReConstruct)
      //annotation of david:这里的互锁边为董事会互锁边
      val tpinWithIL = InterlockTools.addIL(tpin, weight = 0.0, degree = 1).persist()
      val tpinOnlyCompany = InterlockTools.transform(tpinWithIL)
      persist(tpinOnlyCompany,paths)
    }else{
      HdfsTools.getFromObjectFile[InfluVertexAttr, InfluEdgeAttr](sc, paths(0), paths(1))
    }
  }

  /**
    * Author: weiwenda
    * Description:  根据InfluEdgeAttr的边的多个权重，计算新的影响力边
    * 影响力边与初始信用分值结合，返回新的Graph
    * Date: 下午6:52 2017/11/28
    */
  override def adjust(graph: Graph[InfluVertexAttr, InfluEdgeAttr]): Graph[ResultVertexAttr, ResultEdgeAttr] = {
    val savePaths = Seq(s"${hdfsDir}/inf_vertices_${this.getClass.getSimpleName}",
      s"${hdfsDir}/inf_edges_${this.getClass.getSimpleName}")
    //annotation of david:forceReConstruct=true表示强制重新构建TPIN,默认不强制
    if (!HdfsTools.Exist(sc, savePaths(0)) || !HdfsTools.Exist(sc,savePaths(1)) || forceReAdjust) {
      val influenceGraph = computeInfluence(graph).mapVertices((vid, vattr) => (vattr.xyfz, vattr.wtbz))
      //annotation of david:修正后听影响力网络 vertices:93523 edges:1850050
      // fixedGraph: Graph[Int, Double] 点属性为修正后的信用评分，边属性仍为影响力值
      val fixedGraph = computeCreditScore(influenceGraph)
      persist(fixedGraph, savePaths)
    }else{
      HdfsTools.getFromObjectFile[ResultVertexAttr, ResultEdgeAttr] (sc, savePaths(0),savePaths(1))
    }
  }

  override def persist(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): Unit = {
    //annotation of david:bypass=true表示跳过边表的输出
    OracleDBUtil.saveFinalScore(fixedGraph, session, vertex_dst = "WWD_INFLUENCE_RESULT", bypass = true)
  }


  //annotation of david:1.初始化bel和pl 2.选择邻居 3.图结构简化
  def computeInfluence(tpin: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
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
    //annotation of david:路径长度至少为1
    val paths = credit_DS.getPath(initGraph, maxIteratons = 3, initLength = 1).map(e => (e._1, e._2.filter(_.size > 1))).filter(e => e._2.size > 0)
    //        paths.saveAsObjectFile("/tpin/wwd/influence/paths")
    //        sc.objectFile[(VertexId,MessagePropagation.Paths)](verticesFilePath)
    //paths:93523
    //annotation of david:lambda=1表示使用第一种三角范式，bypass=true表示不输出路径用于显示
    val influenceEdge = _influenceOnPath(paths, lambda, session, bypass)
    val influenceGraph = Graph(belAndPl.vertices, influenceEdge).persist()

    //annotation of david:滤除影响力过小的边
    val finalInfluenceGraph = credit_DS.influenceInTotal(influenceGraph)
    finalInfluenceGraph
    //finalInfluenceGraph size: vertices:93523 edges:1850050
  }

  //annotation of david:先对已有评分的节点进行修正，（只拉低）
  def computeCreditScore(influenceGraph: Graph[(Int, Boolean), Double]): Graph[ResultVertexAttr, ResultEdgeAttr] = {
    val fzMessage = influenceGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
      if (ctx.srcAttr._1 > 0 && ctx.dstAttr._1 > 0) {
        if(ctx.srcAttr._1<80)
          ctx.sendToDst(Seq((ctx.srcAttr._1, ctx.attr)))
        if(ctx.dstAttr._1<80)
          ctx.sendToSrc(Seq((ctx.dstAttr._1, ctx.attr)))
      }, _ ++ _).cache()

    val fixAlreadyGraph = influenceGraph.outerJoinVertices(fzMessage) {
      case (vid, vattr, listMessage) =>
        if (listMessage.isEmpty)
          (vattr._1, vattr._1, vattr._2)
        else {
          (vattr._1, _AggregateMessage(vattr, listMessage.get, alpha), vattr._2)
        }
    }.cache()
//    val fzMessage2 = fixAlreadyGraph.aggregateMessages[Seq[(Int, Double)]](ctx =>
//      if (ctx.dstAttr._1 == 0 && ctx.srcAttr._1 > 0) {
//        //(ctx.dstAttr._1 == 0|| ctx.dstAttr._1 >90 ) && ctx.srcAttr._1 > 0 && ctx.srcAttr._1 < 90
//        //annotation of david:分数越低的企业影响力越大
//        val weight = ctx.attr * (100 - ctx.srcAttr._2) * (100 - ctx.srcAttr._2)
//        ctx.sendToDst(Seq((ctx.srcAttr._2, weight)))
//      }, _ ++ _).cache()
//    val fixNotyetGraph = fixAlreadyGraph.outerJoinVertices(fzMessage2) {
//      case (vid, vattr, listMessage) =>
//        if (listMessage.isEmpty)
//          vattr
//        else {
//          (vattr._1, _AggregateMessage(listMessage.get), vattr._3)
//        }
//    }.cache()
//
//    fzMessage.unpersist(blocking = false)
//    fzMessage2.unpersist(blocking = false)
//    fixAlreadyGraph.unpersistVertices(blocking = false)
//    fixAlreadyGraph.edges.unpersist(blocking = false)
    val fixNotyetGraph = fixAlreadyGraph
    val max = fixNotyetGraph.vertices.map(_._2._2).max()
    println(max)
    fixNotyetGraph.mapVertices { case (vid, (old, newfz, wtbz)) => ResultVertexAttr(old, (newfz / max.toDouble * 100).toInt, wtbz) }
      .mapEdges(e => ResultEdgeAttr(e.attr))
  }

  //annotation of david:概率上限
  protected def _computePl(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double, method: Int) = {
    var max = 0D
    credit_DS.method1s.get(method).get match {
      case "maxmin" => max = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).max;
      case "proba" => max = controllerInterSect + tz_bl + kg_bl + jy_bl -
        controllerInterSect * tz_bl - tz_bl * kg_bl - kg_bl * jy_bl - controllerInterSect * kg_bl - controllerInterSect * jy_bl - tz_bl * jy_bl +
        controllerInterSect * tz_bl * kg_bl + controllerInterSect * kg_bl * jy_bl + controllerInterSect * tz_bl * jy_bl + tz_bl * kg_bl * jy_bl -
        controllerInterSect * tz_bl * kg_bl * jy_bl;
      case "bouned" => max = (controllerInterSect + tz_bl + kg_bl + jy_bl).min(1);
      case "ds" => max = 1 - (1 - controllerInterSect) * (1 - tz_bl) * (1 - kg_bl) * (1 - jy_bl) / 2
    }
    if (max > 1)
      max = 1
    max
  }

  //annotation of david:概率下限 对空集进行min会报empty.min错
  protected def _computeBel(controllerInterSect: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double, method: Int) = {
    val tmp = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).filter(_ > 0)
    var min = 0D
    credit_DS.method1s.get(method).get match {
      case "maxmin" =>
        if (tmp.size > 0)
          min = tmp.min
      case "proba" => if (tmp.size > 0) min = tmp.reduce(_ * _);
      case "bouned" => min = (controllerInterSect + tz_bl + kg_bl + jy_bl - 1).max(0);
      case "ds" => min = 1 - (1 - controllerInterSect) * (1 - tz_bl) * (1 - kg_bl) * (1 - jy_bl)
    }
    if (min > 1)
      min = 1
    min
  }

  //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
  protected def _influenceOnPath[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double, InfluEdgeAttr)]]](paths: RDD[(VertexId, T)], lambda: Int, sqlContext: SparkSession, bypass: Boolean) = {
    if (!bypass) {
      val toOutput = paths.flatMap { case (vid, vattr) =>
        val DAG = credit_DS.graphReduce(vattr)
        DAG.filter { case path =>
          val res = path.reduceLeft((a, b) => credit_DS.combineInfluence(a, b, lambda))
          //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
          // 只输出大于0.1pTrust的路径
          res._3 > 0.1
        }
      }
      OracleDBUtil.savePath(toOutput, sqlContext)
    }
    val influences = paths.map { case (vid, vattr) =>
      val DAG = credit_DS.graphReduce(vattr)
      val influenceSinglePath = DAG.map { path =>
        val res = path.reduceLeft((a, b) => credit_DS.combineInfluence(a, b, lambda))
        //annotation of david:pTrust使用t-norm，unc使用pl-bel求平均
        (res._1, res._3, res._4 / (path.size - 1))
      }
      (vid, influenceSinglePath)
    }
      .flatMap { case (vid, list) =>
        list.map { case (dstid, pTrust, unc) => ((vid, dstid), (pTrust, unc)) }
      }.aggregateByKey((0D, 0D))(_combinePath1, _combinePath2).
      map { case ((vid, dstid), (pTrust, total_certainty)) => Edge(vid, dstid, pTrust / total_certainty) }
    influences
  }

  //annotation of david:利用pTrust和unc聚合多路径的影响值，权重比例为 1-unc
  protected def _combinePath1(x: (Double, Double), y: (Double, Double)) = {
    (x._1 + (y._1 * (1 - y._2)), x._2 + 1 - y._2)
  }

  //annotation of david:利用pTrust和unc聚合多路径的影响值
  protected def _combinePath2(x: (Double, Double), y: (Double, Double)) = {
    (x._1 + y._1, x._2 + y._2)
  }

  protected def _AggregateMessage(listMessage: Seq[(Int, Double)]): Int = {
    val totalWeight = listMessage.map(_._2).sum
    var res = 0D
    listMessage.foreach { case (cur_fx, weight) => res += cur_fx * weight / totalWeight }
    res.toInt
  }


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
  protected def _AggregateMessage(xyfz: (Int, Boolean), listMessage: scala.Seq[(Int, Double)], alpha: Double): Int = {
    //        val totalWeight = listMessage.map(_._2).sum
    //        val alpha_fix = alpha+0.001
    //        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
    //        var i = 0
    //        var res = 0D
    //        while (i < Sortedlist.size) {
    //            val (cur_fx, weight) = Sortedlist(i)
    //            res += (xyfz._1 - cur_fx) * weight / totalWeight
    //            i += 1
    //        }
    //        (xyfz._1 - res).toInt

    //        if(( xyfz._1 - (1-alpha_fix)/alpha_fix * res).toInt <= 100) ( alpha * xyfz._1 - (1-alpha_fix)/alpha_fix * res).toInt  else 100

    //        val totalWeight = listMessage.map(_._2).sum
    //        var before = 0D
    //        listMessage.foreach { case (cur_fx, weight) => before += cur_fx * weight / totalWeight }
    //        val result = alpha * xyfz._1/10 + (1-alpha) * before
    //        result.toInt

    val totalWeight = listMessage.map(e => e._2 * (110 - e._1) / 100D).sum
    var before = 0D
    listMessage.foreach { case (cur_fx, weight) => before += cur_fx * weight * (110 - cur_fx) / 100D / totalWeight }
    val result = alpha * xyfz._1 + (1 - alpha) * before
    result.toInt

  }
}

object credit_DS {

  def main(args: Array[String]): Unit = {
    val instance = new credit_DS(forceReConstruct = true)
    instance.run()
    instance.evaluation(PREF())
    instance.persist(instance.fixedGraph)
    val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices", "/tpin/wwd/influence/fixed_edges")
    instance.persist[ResultVertexAttr, ResultEdgeAttr](instance.fixedGraph, outputPaths)
  }
  val method1s = Map(1->"maxmin",2->"proba",3->"bouned",4->"ds",5->"TidalTrust",6->"fuzz")
  val method2s = Map(1 ->"min",2 ->"product",3 -> "Hamacher",4->"Luka")
  type Path = Seq[(VertexId, String, Double, Double, InfluEdgeAttr)]
  type Paths = Seq[Seq[(VertexId, String, Double, Double, InfluEdgeAttr)]]


  def computeCI(srclist: Seq[(String, Double)], dstlist: Seq[(String, Double)], kind: Int): Double = {

    var score = 0.0
    val srcMap = srclist.toMap
    val dstMap = dstlist.toMap
    if (kind == 1) {
      srcMap.keys.toSeq.intersect(dstMap.keys.toSeq).foreach(key =>
        score += srcMap(key).min(dstMap(key))
      )
    } else if (kind == 2) {
      score = srcMap.keys.toSeq.intersect(dstMap.keys.toSeq).size
    }
    score
  }

  //annotation of david:在3个分数之间使用最大值做为控制人亲密度
  def computeCI(srcAttr: InfluVertexAttr, dstAttr: InfluVertexAttr, kind: Int = 1): Double = {
    //        val gd_score = computeCI(srcAttr.gd_list, dstAttr.gd_list, kind)
    //        val zrrtz_score = computeCI(srcAttr.zrrtz_list, dstAttr.zrrtz_list, kind)
    val gd_score = 0D
    val zrrtz_score = 0D
    var fddbr_score = 0D
    if (srcAttr.fddbr.equals(dstAttr.fddbr)) fddbr_score = 1D
    val toReturn = gd_score.max(zrrtz_score).max(fddbr_score)
    toReturn
  }

  //
  //    //annotation of david:归一化企业相关自然人的权重
  //    def fixVertexWeight(tpin: Graph[VertexAttr, EdgeAttr]) = {
  //        val toReurn = tpin.mapVertices { case (vid, vattr) =>
  //            val sum_gd = vattr.gd_list.map(_._2).sum
  //            vattr.gd_list = vattr.gd_list.map { case (gd, weight) => (gd, weight / sum_gd) }
  //            val sum_tz = vattr.zrrtz_list.map(_._2).sum
  //            vattr.zrrtz_list = vattr.zrrtz_list.map { case (tzf, weight) => (tzf, weight / sum_tz) }
  //            vattr
  //        }
  //        toReurn
  //    }

  //annotation of david:决定路径经过哪些邻居,ps:每个企业只影响3家企业
  def selectNeighbor[VD: ClassTag](belAndPl: Graph[VD, DSEdgeAttr], selectTopN: Int = 3): Graph[VD, DSEdgeAttr] = {
    def sendMessage(edge: EdgeContext[VD, DSEdgeAttr, Seq[(VertexId, DSEdgeAttr)]]): Unit = {
      edge.sendToSrc(Seq((edge.dstId, edge.attr)))
    }

    val messages = belAndPl.aggregateMessages[Seq[(VertexId, DSEdgeAttr)]](sendMessage(_), _ ++ _).cache()

    val filtered_edges = messages.map { case (vid, edgelist) =>
      (vid, edgelist.sortBy(_._2.pl)(Ordering[Double].reverse).slice(0, selectTopN))
    }.flatMap { case (vid, edgelist) => edgelist.map(e => Edge(vid, e._1, e._2)) }


    Graph[VD, DSEdgeAttr](belAndPl.vertices, filtered_edges).persist()
  }
  //annotation of david:收集所有长度initlength-1到maxIteration-1的路径
  def getPath(graph: Graph[Paths, DSEdgeAttr], maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Paths, DSEdgeAttr, Paths],
                  length: Int): Unit = {
      val satisfied = edge.dstAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.srcId))
      if (satisfied.size > 0) {
        // 向终点发送顶点路径集合，每个经过节点的id,sbh,当前经过边的bel,pl,原始4维权重
        edge.sendToSrc(satisfied.map(Seq((edge.srcId, edge.attr.src, edge.attr.bel, edge.attr.pl, edge.attr.edgeAttr)) ++ _))
      }
    }

    var preproccessedGraph = graph.cache()
    var i = initLength
    var messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _)
    var activeMessages = messages.count()
    var prevG: Graph[Paths, DSEdgeAttr] = null
    while (activeMessages > 0 && i <= maxIteratons) {
      prevG = preproccessedGraph
      preproccessedGraph = preproccessedGraph.joinVertices[Paths](messages)((id, vd, path) => vd ++ path).cache()
      print("iterator " + i + " finished! ")
      i += 1
      val oldMessages = messages
      messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _).cache()
      try {
        activeMessages = messages.count()
      } catch {
        case ex: Exception =>
          println("又发生异常了")
      }
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
    }
    //         printGraph[Paths,Int](preproccessedGraph)
    preproccessedGraph.vertices
  }
  /**
   *Author:weiwenda
   *Description:泛型版的选择TopN邻居
    *  belAndPl为需要简化的图
    *  getWeight是从边属性中选择比较项的带入函数
    *  selectTopN为所要选择的N
   *Date:17:22 2017/12/21
   */
  def simpleGraph[VD: ClassTag,ED:ClassTag](belAndPl: Graph[VD,ED],getWeight:((Long,ED))=>Double, selectTopN: Int = 20): Graph[VD, ED] = {
    def sendMessage(edge: EdgeContext[VD, ED, Seq[(VertexId, ED)]]): Unit = {
      edge.sendToSrc(Seq((edge.dstId, edge.attr)))
    }

    val messages = belAndPl.aggregateMessages[Seq[(VertexId, ED)]](sendMessage(_), _ ++ _).cache()

    val filtered_edges = messages.map { case (vid, edgelist) =>
      (vid, edgelist.sortBy[Double](getWeight)(Ordering[Double].reverse).slice(0, selectTopN))
    }.flatMap { case (vid, edgelist) => edgelist.map(e => Edge(vid, e._1, e._2)) }


    Graph[VD, ED](belAndPl.vertices, filtered_edges).persist()
  }
  /**
   *Author:weiwenda
   *Description:泛型版收集所有长度initlength到maxIteration的路径
    * graph为输入图
    * sendMsg用于带入每次所发消息
    * reduceMsg用于点层聚合消息，一般为_++_
    * maxIteratons 与 initLength共同决定迭代终点，最终的路径Seq长度为maxIteratons+1,最短的路径Seq长度为initLength
   *Date:17:25 2017/12/21
   */
  def getPathGeneric[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED],
              sendMsg:(EdgeContext[VD, ED,VD], Int)=>Unit,
              reduceMsg:(VD,VD)=>VD,
              maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
    // 发送路径
    var preproccessedGraph = graph.cache()
    var i = initLength
    var messages = preproccessedGraph.aggregateMessages[VD](sendMsg(_, i),reduceMsg)
    var activeMessages = messages.count()
    var prevG: Graph[VD, ED] = null
    while (activeMessages > 0 && i <= maxIteratons) {
      prevG = preproccessedGraph
      preproccessedGraph = preproccessedGraph.joinVertices[VD](messages)((id, vd, path) => reduceMsg(vd,path)).cache()
      print("iterator " + i + " finished! ")
      i += 1
      if(i<=maxIteratons){
        val oldMessages = messages
        messages = preproccessedGraph.aggregateMessages[VD](sendMsg(_, i), reduceMsg).cache()
        try {
          activeMessages = messages.count()
        } catch {
          case ex: Exception =>
            println("又发生异常了")
        }
        oldMessages.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
    }
    //         printGraph[Paths,Int](preproccessedGraph)
    preproccessedGraph.vertices
  }
  //annotation of david:针对图结构的shared segment和crossing segment进行修改TODO
  def graphReduce[T <: Iterable[Seq[(graphx.VertexId, String, Double, Double, EdgeAttr)]]](vattr: T): T = {
    vattr
  }

  //annotation of david:使用frank t-norm聚合路径上的影响值，vid,bel,pl
  def combineInfluence(x: (VertexId, String, Double, Double, InfluEdgeAttr),
                       y: (VertexId, String, Double, Double, InfluEdgeAttr),
                       lambda: Int): (VertexId, String, Double, Double, InfluEdgeAttr) = {
    val (vid,pTrust) = combineInfluence((x._1,x._4),(y._1,y._4),lambda)
    val unc = x._4 + y._4 - y._3
    (vid, "", pTrust, unc, y._5)
  }
  def combineInfluence(x: (VertexId, Double), y: (VertexId, Double), lambda: PartitionID):(VertexId,Double) = {
    val plambda = 0.001
    var pTrust = 0D
    val a = x._2
    val b = y._2
    if (lambda == 1) pTrust = a.min(b)
    else if (lambda == 2) pTrust = a * b
    //        else if (lambda == Integer.MAX_VALUE) pTrust = (a + b - 1).max(0.0)
    else if (lambda == 3) pTrust = (a * b) / (a + b - a * b)
    else if (lambda == 4) pTrust = (a + b - 1).max(0.0)
    else pTrust = Math.log(1 + (((Math.pow(plambda, a) - 1) * ((Math.pow(plambda, b) - 1))) / (plambda - 1))) / Math.log(plambda)
    (y._1,pTrust)
  }

  //annotation of david:增加考虑源点企业和终点企业的控制人亲密度，以及多条路径，得到标量的影响值，加权平均
  def influenceInTotal(influenceGraph: Graph[InfluVertexAttr, Double]) = {
    val toReturn = influenceGraph.mapTriplets { case triplet =>
      val controllerInterSect = computeCI(triplet.srcAttr, triplet.dstAttr)
      triplet.attr.max(controllerInterSect)
    }.subgraph(epred = triplet => triplet.attr > 0.01)
    toReturn
  }
}


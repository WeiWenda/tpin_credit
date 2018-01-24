package wwd.strategy

import wwd.evaluation.{EvaluateResult, Measurement}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import wwd.strategy.impl.{ResultEdgeAttr, ResultVertexAttr}
import wwd.utils.{HdfsTools, OracleTools}

import scala.collection.Seq
import scala.reflect.ClassTag
/**
* Author: weiwenda
* Description:  ALRunner提供了代码基础框架
*               1.初始化sc和session，并持有对象
*               2.抽象方法getGraph，作为程序输入
*               3.抽象方法adjust，作为程序计算主体
*               4.抽象方法persist，作为程序输出
*               5.抽象方法description,作为当前adjust的方法描述
*               6.模板方法run，完整地调用一次getGraph和persist
*               7.普通方法evaluation，针对当前计算结果，统计一个或多个评价指标
* Date: 下午8:11 2017/11/29
*/
abstract class ALRunner[VD1: ClassTag, ED1: ClassTag, VD2: ClassTag, ED2: ClassTag]() extends Serializable{
  @transient
  var sc: SparkContext = _
  @transient
  var session: SparkSession = _
  @transient
  var primitiveGraph: Graph[VD1, ED1] = _
  @transient
  var fixedGraph: Graph[VD2, ED2] = _

  initialize()

  def initialize(): Unit = {
    session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sc = session.sparkContext
  }
  def description:String

  def getGraph(sc: SparkContext, session: SparkSession): Graph[VD1, ED1]

  def adjust(value: Graph[VD1, ED1]): Graph[VD2, ED2]

  def run(): Unit = {
    //annotation of david:加入缓存机制，如果primitiveGraph已加载则跳过
    if(primitiveGraph == null)
      primitiveGraph = getGraph(sc, session)
    fixedGraph = adjust(primitiveGraph)
  }
/**
* Author: weiwenda
* Description: 同时处理一组统计指标，返回一个a|b,c|d格式的字符串
* Date: 下午4:06 2017/11/29
*/
  def evaluation[T](measurements: Seq[Measurement[VD2,ED2,EvaluateResult[T]]],separator1:String="|",separator2:String=","):String = {
    measurements.map(measurement=>measurement.compute(fixedGraph).toString(separator1)).mkString(separator2)
  }


  def evaluation[RD](measurement: Measurement[VD2, ED2, RD]): RD = {
    measurement.compute(fixedGraph)
  }

  def persist[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED], outputPaths: Seq[String]): Graph[VD, ED] = {
    HdfsTools.saveAsObjectFile(graph, sc, outputPaths(0), outputPaths(1))
    HdfsTools.getFromObjectFile[VD, ED](sc, outputPaths(0), outputPaths(1))
  }

  def persist(graph: Graph[VD2, ED2]): Unit

  def showDimension[VD,ED](graph:Graph[VD,ED],title:String)

}

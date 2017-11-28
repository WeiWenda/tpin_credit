package wwd.strategy

import wwd.evaluation.{EvaluateResult, Measurement}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import wwd.utils.{HdfsTools, OracleDBUtil}

import scala.reflect.ClassTag

abstract class ALRunner[VD1: ClassTag, ED1: ClassTag,VD2:ClassTag,ED2:ClassTag] {
  protected var sc: SparkContext = _
  protected var session : SparkSession = _

  protected var primitiveGraph :Graph[VD1,ED1] = _
  protected var fixedGraph :Graph[VD2,ED2] = _

  def initialize(): Unit = {
    session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sc = session.sparkContext
  }
  def getGraph(sc: SparkContext, session: SparkSession):Graph[VD1, ED1]
  def adjust(value: Graph[VD1, ED1]):Graph[VD2, ED2]

  def run(): Unit = {
    initialize()
    primitiveGraph = getGraph(sc,session)
    fixedGraph = adjust(primitiveGraph)
  }

  def evaluation[RD](measurement: Measurement[VD2,ED2,RD]):RD ={
    measurement.compute(fixedGraph)
  }

  def persist[VD,ED](graph:Graph[VD,ED],outputPaths:Seq[String]): Unit ={
    HdfsTools.saveAsObjectFile(graph,sc,outputPaths(0),outputPaths(1))
  }

  def persist(graph:Graph[VD2,ED2]):Unit

}

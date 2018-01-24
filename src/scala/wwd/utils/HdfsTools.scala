package wwd.utils

import _root_.java.math.BigDecimal
import _root_.java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import wwd.entity._
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr, WholeEdgeAttr, WholeVertexAttr}

import scala.reflect.ClassTag

/**
  * Created by weiwenda on 2017/3/15.
  */
object HdfsTools {
  // 保存TPIN到HDFS
  def saveAsObjectFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                               verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd"): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(30).saveAsObjectFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(30).saveAsObjectFile(edgesFilePath)
  }

  // 保存TPIN到HDFS
  def saveAsTextFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                             verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd"): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(1).saveAsTextFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(1).saveAsTextFile(edgesFilePath)
  }

  def checkDirExist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(outpath), true)
    }
    catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  // 从HDFS获取TPIN
  def getFromObjectFile[VD:ClassTag, ED:ClassTag](sparkContext: SparkContext, verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd")
  : Graph[VD, ED] = {
    // 对象方式获取顶点集
    val vertices = sparkContext.objectFile[(VertexId, VD)](verticesFilePath).repartition(30)
    // 对象方式获取边集
    val edges = sparkContext.objectFile[Edge[ED]](edgesFilePath).repartition(30)
    // 构建图
    Graph[VD,ED](vertices, edges)
  }

  def getFromCsv(sc: SparkContext, vertexPath: String, edgePath: String): Graph[InfluVertexAttr, InfluEdgeAttr] = {
    //    val edgesTxt=sc.textFile("file:///home/david/IdeaProjects/Find_IL_Edge/lib/InputCsv/edges.csv")
    //    val vertexTxt=sc.textFile("file:///home/david/IdeaProjects/Find_IL_Edge/lib/InputCsv/vertices.csv")
    val edgesTxt = sc.textFile(edgePath)
    val vertexTxt = sc.textFile(vertexPath)
    val vertices = vertexTxt.filter(!_.startsWith("id")).map(_.split(",")).map {
      case node =>
        var i = 3
        var gd_list: Seq[(String, Double)] = Seq()
        var zrrtz_list: Seq[(String, Double)] = Seq()
        while (i < node.size) {
          if (node(i).startsWith("股东"))
            gd_list ++= Seq((node(i), node(i + 1).toDouble))
          else if (node(i).startsWith("投资方"))
            zrrtz_list ++= Seq((node(i), node(i + 1).toDouble))
          i += 2
        }
        val vertex = InfluVertexAttr(node(1), node(2))
        vertex.xyfz = node.last.toInt
        (node(0).toLong, vertex)
    }

    val edges = edgesTxt.filter(!_.startsWith("source")).map(_.split(",")).map {
      case e =>
        val eattr = InfluEdgeAttr()
        eattr.tz_bl = e(2).toDouble
        eattr.jy_bl = e(3).toDouble
        eattr.kg_bl = e(4).toDouble
        Edge(e(0).toLong, e(1).toLong, eattr)
    }
    Graph(vertices, edges)
  }

  def printGraph[VD, ED](graph: Graph[VD, ED]) = {
    graph.vertices.collect().foreach {
      println
    }
    graph.edges.collect().foreach { case edge => println(edge) }
  }

  def Exist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    hdfs.exists(new Path(outpath))
  }
}

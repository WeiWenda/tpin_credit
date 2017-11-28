package wwd.main

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import wwd.entity.impl.{InfluVertexAttr, WholeEdgeAttr, WholeVertexAttr}
import wwd.entity.{EdgeAttr, VertexAttr, WholeEdgeAttr, WholeVertexAttr}
import wwd.utils.xyshow.XYShowTools
import wwd.utils._

/**
  * Created by weiwenda on 2017/5/10.
  */
object XYShow {
    def run(sc: SparkContext, hiveContext: HiveContext) {
        val format = new SimpleDateFormat("hh:mm:ss.SSS")

        val beforeConstruct = new Date()
        println("\r构图程序开始执行：" + format.format(beforeConstruct))
        val tpin = HdfsTools.getFromOracleTable2(hiveContext).persist()
        println("\nafter construct:  \n" + tpin.vertices.count)
        println(tpin.edges.count)
        HdfsTools.saveAsObjectFile(tpin,sc,"/tpin/wwd/influence/whole_vertices","/tpin/wwd/influence/whole_edges")
        val tpinFromObject = HdfsTools.getFromObjectFile[WholeVertexAttr,WholeEdgeAttr](sc,"/tpin/wwd/influence/whole_vertices","/tpin/wwd/influence/whole_edges")
        println(tpinFromObject.vertices.count)
        println(tpinFromObject.edges.count)
        OracleDBUtil.saveWholeVertex(tpinFromObject.vertices,hiveContext,dst="WWD_XYSHOW_VERTEX_INIT")
        OracleDBUtil.saveWholeEdge(tpinFromObject.triplets,hiveContext,dst="WWD_XYSHOW_EDGE_INIT")

        val tpinWithIL = XYShowTools.addIL(tpinFromObject,weight = 0.0,degree = 1).persist()

        val tpinOnlyCompany = XYShowTools.transform(tpinWithIL)

        HdfsTools.saveAsObjectFile(tpinOnlyCompany,sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        val tpin1 = HdfsTools.getFromObjectFile[InfluVertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        //annotation of david:影响力网络构建成功 influenceGraph: Graph[Int, Double]，点属性为信用评分，边属性为影响力值
        val influenceGraph = MessagePropagation.run(tpin1,hiveContext,bypass=true).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz))
        HdfsTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")
        val influenceGraph1 = HdfsTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")

        //annotation of david:修正后听影响力网络 vertices:93523 edges:1850050
        // fixedGraph: Graph[Int, Double] 点属性为修正后的信用评分，边属性仍为影响力值
        val fixedGraph = CombineNSXY.run(influenceGraph1)

        val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")
        HdfsTools.saveAsObjectFile(fixedGraph,sc,outputPaths(0),outputPaths(1))

        val finalScore = HdfsTools.getFromObjectFile[(Int,Int,Boolean),Double](sc,"/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")

        OracleDBUtil.saveFinalScore(finalScore,hiveContext)





        //        val beforeAddIL = new Date()
//        println("\r读取数据结束，即将添加企业连边：" + format.format(beforeAddIL))
//
//        println("\nafter add cid:\n " + tpinWithIL.vertices.count)
//        println(tpinWithIL.edges.count)
//        InputOutputTools.saveAsHiveTable(hiveContext, tpinWithIL)
//        val afterOutput = new Date();
//        println("\r输出到Hive结束，各阶段用时如下：" + format.format(afterOutput))
//
//        println("初始建图用时：" + (beforeAddIL.getTime() - beforeConstruct.getTime()) / (1000D) + "秒")
//        println("添加连边及输出至Hive用时：" + (afterOutput.getTime() - beforeAddIL.getTime()) / (1000D) + "秒")
    }

}

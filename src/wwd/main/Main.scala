package wwd.main

import java.util.Date

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import wwd.entity.{WholeEdgeAttr, WholeVertexAttr, EdgeAttr, VertexAttr}
import wwd.utils.xyshow.XYShowTools
import wwd.utils._

/**
  * Created by weiwenda on 2017/3/20.
  */
object Main{
    def main(args:Array[String]){

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        generateInfluence(sc,hiveContext)

        val fixedGraph = withDS(sc,hiveContext)

        val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")
        InputOutputTools.saveAsObjectFile(fixedGraph,sc,outputPaths(0),outputPaths(1))
        val finalScore = InputOutputTools.getFromObjectFile[(Int,Int,Boolean),Double](sc,"/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")

        Experiments.computePref_new(finalScore)

        OracleDBUtil.saveFinalScore(finalScore,hiveContext,vertex_dst ="WWD_INFLUENCE_RESULT",bypass = true)

    }
    def generateInfluence(sc:SparkContext,hiveContext: HiveContext): Unit ={
        if(!InputOutputTools.Exist(sc,"/tpin/wwd/influence/vertices")){
            val tpin = InputOutputTools.getFromOracleTable2(hiveContext).persist()
            println("\nafter construct:  \n" + tpin.vertices.count)
            println(tpin.edges.count)
            InputOutputTools.saveAsObjectFile(tpin,sc,"/tpin/wwd/influence/whole_vertices","/tpin/wwd/influence/whole_edges")
        }
        val tpinFromObject = InputOutputTools.getFromObjectFile[WholeVertexAttr,WholeEdgeAttr](sc,"/tpin/wwd/influence/whole_vertices","/tpin/wwd/influence/whole_edges")

        //annotation of david:这里的互锁边为董事会互锁边
        val tpinWithIL = XYShowTools.addIL(tpinFromObject,weight = 0.0,degree = 1).persist()
        val tpinOnlyCompany = XYShowTools.transform(tpinWithIL)
        InputOutputTools.saveAsObjectFile(tpinOnlyCompany,sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
    }
    def withDS(sc:SparkContext,hiveContext: HiveContext): Graph[(Int, Int, Boolean), Double]={
        //annotation of david:tpin1: Graph[VertexAttr, EdgeAttr]
        val tpin1 = InputOutputTools.getFromObjectFile[VertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        //annotation of david:影响力网络构建成功 influenceGraph: Graph[Int, Double]，点属性为信用评分，边属性为影响力值
        val influenceGraph = MessagePropagation.run(tpin1,hiveContext,bypass=true).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz))
        InputOutputTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")
        val influenceGraph1 = InputOutputTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")
        //annotation of david:修正后听影响力网络 vertices:93523 edges:1850050
        // fixedGraph: Graph[Int, Double] 点属性为修正后的信用评分，边属性仍为影响力值
        val fixedGraph = CombineNSXY.run(influenceGraph1)
        fixedGraph

    }


}

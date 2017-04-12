package wwd.main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import wwd.entity.{EdgeAttr, VertexAttr}
import wwd.utils.{CombineNSXY, MessagePropagation, InputOutputTools}

/**
  * Created by weiwenda on 2017/3/20.
  */
object Main{
    def main(args:Array[String]){

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
        if(!InputOutputTools.Exist(sc,"/tpin/wwd/influence/vertices")){
            val tpin = InputOutputTools.getFromOracleTable(hiveContext)
            InputOutputTools.saveAsObjectFile(tpin,sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        }
        val tpin1 = InputOutputTools.getFromObjectFile[VertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")

        //annotation of david:影响力网络构建成功
        val influenceGraph = MessagePropagation.run(tpin1).mapVertices((vid,vattr)=>vattr.xyfz)
        InputOutputTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")
        val influenceGraph1 = InputOutputTools.getFromObjectFile[Int,Double](sc,"/tpin/wwd/influence/inf_vertices","/tpin/wwd/influence/inf_edges")

        //annotation of david:修正后听影响力网络 vertices:93523 edges:1850050
        val fixedGraph = CombineNSXY.run(influenceGraph1)

        val outputPaths = Seq("/tpin/wwd/influence/fixed_vertices","/tpin/wwd/influence/fixed_edges")
        InputOutputTools.saveAsObjectFile(fixedGraph,sc,outputPaths(0),outputPaths(1))











    }


}

package wwd.main

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import wwd.utils.{MessagePropagation, InputOutputTools}

/**
  * Created by weiwenda on 2017/3/20.
  */
object Main{
    def main(args:Array[String]){

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        val tpin = InputOutputTools.getFromOracleTable(hiveContext)

        InputOutputTools.saveAsObjectFile(tpin,sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")

        val tpin1 = InputOutputTools.getFromObjectFile(sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")

        //annotation of david:影响力网络构建成功
        val influenceGraph = MessagePropagation.run(tpin1)











    }


}

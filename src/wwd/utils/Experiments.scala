package wwd.utils

import java.math.BigDecimal
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import wwd.entity.{EdgeAttr, VertexAttr}

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {

    def getHitedNum(badList: RDD[(VertexId, String)], tuples: Array[VertexId]) = {
        badList.filter(e=> tuples.contains(e._1)).count()
    }

    def checkEveryN(sc:SparkContext, hiveContext: HiveContext, verticesFilePath:String,beforePath :String ="/tpin/wwd/influence/vertices"): Unit ={

        //annotation of david: badList: RDD[(Long, String)] wt_vertexid,nsrdzdah
        val badList = getTroubleList(hiveContext)

        //annotation of david: verticesAfter: RDD[(graphx.VertexId, Int)] vertexid xyfx_after
        val verticesAfter = sc.objectFile[(VertexId,Int)](verticesFilePath).repartition(200).
            filter(_._2>0).
            sortBy(e=>e._2)

        //annotation of david: verticesBefore: RDD[(graphx.VertexId, Int)] vertexid,xyfz_before
        val verticesBefore = sc.objectFile[(VertexId,VertexAttr)](beforePath).repartition(200).
//            filter(_._2.xyfz>0).
            map(e=> (e._1,e._2.xyfz)).
            sortBy(_._2)

       //annotation of david: output : RDD[(Long, Int, Int,Boolean)] vertexid,xyfx_after,xyfz_before,wtbz
        val output = verticesAfter.join(verticesBefore).leftOuterJoin(badList).map{case (vid,(xyfz,opt)) =>
            if(opt.isEmpty) (vid,xyfz._1,xyfz._2,false)
            else (vid,xyfz._1,xyfz._2,true)
        }
        OracleDBUtil.saveAsOracleTable(output,hiveContext)
        println("output finished!")



//        val fw = CSVProcess.openCsvFileHandle(s"/root/Documents/wwd/Result/shannxiInfluence_${step}.csv",false)
//        for(i <- Range(0,math.ceil(sorted.length/step).toInt)){
//            val num1 = getHitedNum(badList,sorted.slice(step*i,(step*(i+1)).min(sorted.length)))
//            var num2 = 0L
//            if((step*i)<verticesBefore.length){
//                num2 = getHitedNum(badList,verticesBefore.slice(step*i,(step*(i+1)).min(verticesBefore.length)))
//            }
//            println(i+","+num1+","+num2)
//            CSVProcess.saveAsCsvFile(fw,i+","+num1+","+num2)
//        }
//        CSVProcess.closeCsvFileHandle(fw)
    }


    def getTroubleList(hiveContext: HiveContext) = {

        import hiveContext.implicits._
        val all = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.JC_AJXX",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().
            filter($"AJLY_DM".isin(20,92,40)).
            filter($"WFWZLX_DM".isNotNull).
            select("NSRDZDAH", "LRRQ").rdd.
            filter(row=> row.getAs[Date]("LRRQ").getYear()+1900 >= 2009 && row.getAs[Date]("LRRQ").getYear()+1900 <= 2015).
            map { row => (row.getAs[BigDecimal]("NSRDZDAH").toString, 1) }.distinct()
        val nsrdzdahInfo = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_FDDBR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().select("NSRDZDAH", "VERTEXID").
            rdd.repartition(120).map(row => ((row.getAs[BigDecimal]("NSRDZDAH").toString, (row.getAs[BigDecimal]("VERTEXID").longValue()))))

        //annotation of david:按纳税人档案号 得到 顶点编号
        val all_vid = nsrdzdahInfo.join(all).map { case (nsrdzdah, (vid, useless)) => (vid, nsrdzdah) }
        all_vid
    }
    def getNoTroubleList(hiveContext: HiveContext) = {

        import hiveContext.implicits._
        val all = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.JC_AJXX",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().
            //            filter($"AJLY_DM".isin(20,92,40)).
            filter($"WFWZLX_DM".isNull).
            select("NSRDZDAH", "LRRQ").rdd.
            filter(row=> row.getAs[Date]("LRRQ").getYear()+1900 >= 2009 && row.getAs[Date]("LRRQ").getYear()+1900 <= 2015).
            map { row => (row.getAs[BigDecimal]("NSRDZDAH").toString, 1) }.distinct()
        val nsrdzdahInfo = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_FDDBR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().select("NSRDZDAH", "VERTEXID").
            rdd.repartition(120).map(row => ((row.getAs[BigDecimal]("NSRDZDAH").toString, (row.getAs[BigDecimal]("VERTEXID").longValue()))))

        //annotation of david:按纳税人档案号 得到 顶点编号
        val all_vid = nsrdzdahInfo.join(all).map { case (nsrdzdah, (vid, useless)) => (vid, nsrdzdah) }
        all_vid
    }

}

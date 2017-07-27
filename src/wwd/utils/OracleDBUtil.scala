package wwd.utils


import _root_.java.util.Properties
import _root_.java.text.DecimalFormat
import java.Parameters
import org.apache.log4j.receivers.db.dialect.MySQLDialect
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{types, Row, SQLContext}
import wwd.entity.{EdgeAttr, VertexAttr, WholeEdgeAttr, WholeVertexAttr}
import wwd.utils.java.DataBaseManager

/**
  * Oracle数据库存取工具
  *
  * Created by yzk on 2017/4/1.
  */
object OracleDBUtil {

    //  sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    //  sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    val url = Parameters.DataBaseURL
    val user = Parameters.DataBaseUserName
    val password = Parameters.DataBaseUserPassword
    val driver = Parameters.JDBCDriverString

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)


    def saveWholeVertex(vertexs: VertexRDD[WholeVertexAttr], sqlContext: SQLContext, dst: String = "WWD_XYCD_VERTEX") = {
        DataBaseManager.execute("truncate table " + dst)
        val schema = StructType(
            List(
                StructField("ID", LongType, true),
                StructField("NAME", StringType, true),
                StructField("SBH", StringType, true),
                StructField("ISHUMAN", StringType, true),
                StructField("XYFZ", IntegerType, true),
                StructField("XYDJ", StringType, true)
            )
        )
        val rowRDD = vertexs.map { case (vid, attr) => Row(vid, attr.name.substring(0, 80.min(attr.name.length)), attr.nsrsbh, attr.ishuman.toString, attr.xyfz, attr.xydj) }
        val vertexDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(30)
        JdbcUtils.saveTable(vertexDataFrame, url, dst, properties)
    }

    def zipLabel(w_control: Double, w_gd: Double, w_tz: Double, w_trade: Double): String = {
        var toReturn = ""
        val formater = new DecimalFormat("#.###")
        if(w_control>0){
            toReturn += "法人；"
        }
        if(w_tz>0){
            toReturn += "投资："+formater.format(w_tz)+"；"
        }
        if(w_gd>0){
            toReturn += "控股："+formater.format(w_gd)+"；"
        }
        if(w_trade>0){
            toReturn += "交易："+formater.format(w_trade)+"；"
        }
        return toReturn.substring(0,toReturn.length-1)
    }

    def saveWholeEdge(edges: RDD[EdgeTriplet[WholeVertexAttr, WholeEdgeAttr]], sqlContext: SQLContext, dst: String = "WWD_XYCD_EDGE") = {
        DataBaseManager.execute("truncate table " + dst)
        val schema = StructType(
            List(
                StructField("SRC_ID", LongType, true),
                StructField("SRC_NSRDZDAH",StringType,true),
                StructField("DST_ID", LongType, true),
                StructField("DST_NSRDZDAH",StringType,true),
                StructField("CONTROL_WEIGHT", DoubleType, true),
                StructField("GD_WEIGHT", DoubleType, true),
                StructField("INVESTMENT_WEIGHT", DoubleType, true),
                StructField("TRADE_WEIGHT", DoubleType, true),
                StructField("TRADE_JE", DoubleType, true),
                StructField("TRADE_SE", DoubleType, true),
                StructField("LABEL",StringType,true)
            )
        )

        val rowRDD = edges.map(p => Row(p.srcId,p.srcAttr.nsrsbh,
            p.dstId,p.dstAttr.nsrsbh, p.attr.w_control, p.attr.w_gd, 
            p.attr.w_tz, p.attr.w_trade, p.attr.trade_je, p.attr.se,zipLabel(p.attr.w_control, p.attr.w_gd,
                p.attr.w_tz, p.attr.w_trade)))
        val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(30)
        JdbcUtils.saveTable(edgeDataFrame, url, dst, properties)
    }

    def saveFinalScore(finalScore: Graph[(Int,Int,Boolean), Double],sqlContext: SQLContext,
                       edge_dst:String="WWD_XYCD_EDGE_FINAL",vertex_dst:String="WWD_XYCD_VERTEX_FINAL",bypass:Boolean=false): Unit = {
        if(!bypass){
            DataBaseManager.execute("truncate table " + edge_dst)
            val schema = StructType(
                List(
                    StructField("source", LongType, true),
                    StructField("target", LongType, true),
                    StructField("FINAL_INFLUENCE", DoubleType, true)
                )
            )
            val rowRDD = finalScore.edges.map(e=> Row(e.srcId,e.dstId,e.attr)).distinct()

            val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(3)
            JdbcUtils.saveTable(edgeDataFrame, url, edge_dst, properties)
        }

        DataBaseManager.execute("truncate table " + vertex_dst)
        val schema1 = StructType(
            List(
                StructField("VERTICE", LongType, true),
                StructField("INITSCORE", IntegerType, true),
                StructField("FINALSCORE", IntegerType, true),
                StructField("WTBZ",StringType,true)
            )
        )
        val rowRDD1 = finalScore.vertices.map(p => Row(p._1,p._2._1,p._2._2,if(p._2._3)"Y" else "N")).distinct()
        val vertexDataFrame = sqlContext.createDataFrame(rowRDD1, schema1).repartition(3)
        JdbcUtils.saveTable(vertexDataFrame, url, vertex_dst, properties)
    }

    def savePath(toOutput: RDD[Seq[(VertexId,String, Double, Double,EdgeAttr)]],sqlContext: SQLContext,edge_dst:String="WWD_XYCD_EDGE_ML",vertex_dst:String="WWD_XYCD_VERTEX_ML") = {
        DataBaseManager.execute("truncate table " + edge_dst)
        val schema = StructType(
            List(
                StructField("company", LongType, true),
                StructField("source", LongType, true),
                StructField("target", LongType, true),
                StructField("bel", DoubleType, true),
                StructField("pl", DoubleType, true),
                StructField("label",StringType,true),
                StructField("COMPANY_NSRDZDAH", StringType, true),
                StructField("SRC_NSRDZDAH", StringType, true),
                StructField("DST_NSRDZDAH", StringType, true)
            )
        )
        val rowRDD = toOutput.flatMap{p=>
            val company_id = p.last._1
            val company_nsrdzdah = p.last._2
            val a =Seq[Seq[(VertexId, String, Double, Double,EdgeAttr)]]()++ p.sliding(2).filter(_.size == 2)
            a.map(e=> Row(company_id,e(0)._1,e(1)._1,e(0)._3,e(0)._4,e(0)._5.toString,company_nsrdzdah,e(0)._2,e(1)._2))
        }.distinct()
        val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(3)
        JdbcUtils.saveTable(edgeDataFrame, url, edge_dst, properties)
        DataBaseManager.execute("truncate table " + vertex_dst)
        val schema1 = StructType(
            List(
                StructField("COMPANY", LongType, true),
                StructField("COMPANY_NSRDZDAH", StringType, true),
                StructField("VERTICE", LongType, true),
                StructField("NSRDZDAH", StringType, true)
            )
        )
        val rowRDD1 = toOutput.
            flatMap(e=>e.map(ee=>(e.last._1,e.last._2,ee._1,ee._2))).
            distinct().
            map(p => Row(p._1,p._2,p._3,p._4))
        val vertexDataFrame = sqlContext.createDataFrame(rowRDD1, schema1).repartition(3)
        JdbcUtils.saveTable(vertexDataFrame, url, vertex_dst, properties)

    }

}

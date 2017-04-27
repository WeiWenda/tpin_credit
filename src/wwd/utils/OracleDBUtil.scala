package wwd.utils

import java.util.Properties

import org.apache.log4j.receivers.db.dialect.MySQLDialect
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Oracle数据库存取工具
  *
  * Created by yzk on 2017/4/1.
  */
object OracleDBUtil {
    //  sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    //  sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    val url = "jdbc:oracle:thin:@202.117.16.32:1521:shannxi"
    val user = "shannxi"
    val password = "shannxi"
    val driver = "oracle.jdbc.driver.OracleDriver"

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)

    /**
      * 将修正前后的信用评分输出到oracle中
      **/
    def saveAsOracleTable(rdd:  RDD[(Long, Int, Int,String,String)], sqlContext: SQLContext) = {
        val schema = StructType(
            List(
                StructField("VertexId", LongType, true),
                StructField("xyfz_after", IntegerType, true),
                StructField("xyfz_before", IntegerType, true),
                StructField("wtbz", StringType, true),
                StructField("ajly_dm",StringType,true)
            )
        )
        val rowRDD = rdd.map(x => Row(x._1, x._2, x._3,x._4,x._5))

        val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

        JdbcUtils.saveTable(dataFrame, url, "shannxi.WWD_INFLUENCE_RESULT", properties)
    }
    def saveAllVertex(vertexs: RDD[(String, String, Double, Double, Double)],sqlContext: SQLContext) = {
        val schema = StructType(
            List(
                StructField("COMPANY", StringType, true),
                StructField("VERTICE", StringType, true),
                StructField("INFLUENCE", DoubleType, true),
                StructField("INITSCORE", DoubleType, true),
                StructField("FINALSCORE", DoubleType, true)
            )
        )
        val rowRDD = vertexs.map(p => Row(p._1, p._2, p._3, p._4,p._5))
        val vertexDataFrame = sqlContext.createDataFrame(rowRDD, schema)
        JdbcUtils.saveTable(vertexDataFrame, url, "WWD_XYCD_VERTEX", properties)
    }

    def saveAllEdge(edges: RDD[(String, String, String, Double)],sqlContext: SQLContext) = {
        val schema = StructType(
            List(
                StructField("company", StringType, true),
                StructField("source", StringType, true),
                StructField("target", StringType, true),
                StructField("trade_weight", DoubleType, true),
                StructField("influence",DoubleType,true)
            )
        )

        val rowRDD = edges.map(p => Row(p._1, p._2, p._3, p._4,p._4))
        val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema)
        JdbcUtils.saveTable(edgeDataFrame, url, "WWD_XYCD_EDGE", properties)
    }

}

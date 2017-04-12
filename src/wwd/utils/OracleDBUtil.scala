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
    def saveAsOracleTable(rdd:  RDD[(Long, Int, Int,Boolean)], sqlContext: SQLContext) = {
        val schema = StructType(
            List(
                StructField("VertexId", LongType, true),
                StructField("xyfz_after", IntegerType, true),
                StructField("xyfz_before", IntegerType, true),
                StructField("wtbz", StringType, true)
            )
        )
        val rowRDD = rdd.map(x => Row(x._1, x._2, x._3,x._4.toString))

        val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

        JdbcUtils.saveTable(dataFrame, url, "shannxi.WWD_INFLUENCE_RESULT", properties)
    }
}

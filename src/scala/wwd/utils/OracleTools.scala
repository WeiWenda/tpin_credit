package wwd.utils

import _root_.java.text.DecimalFormat
import java.math.BigDecimal

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession, types}
import org.apache.spark.storage.StorageLevel
import wwd.entity.impl.{InfluEdgeAttr, InfluVertexAttr, WholeEdgeAttr, WholeVertexAttr}
import wwd.entity.{EdgeAttr, VertexAttr}
import wwd.main.{Experiments, OOVertexAttr}
import wwd.strategy.impl.{ResultEdgeAttr, ResultVertexAttr}

/**
  * Oracle数据库存取工具
  *
  * Created by yzk on 2017/4/1.
  */
object OracleTools {

  //  sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  //  sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

  val options = Map("url"->Parameters.DataBaseURL,
    "driver"->Parameters.JDBCDriverString,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword)

  def saveNeighborInfo(result: RDD[OOVertexAttr],session: SparkSession,dst:String="WWD_NEIGHBOR_INFO"): Unit = {
    DataBaseManager.execute("truncate table " + dst)
    import session.implicits._
    val rowRDD = result.toDF()
    JdbcUtils.saveTable(rowRDD,Option(rowRDD.schema), false, new JDBCOptions(options+(("dbtable",dst ))))
  }
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
    JdbcUtils.saveTable(vertexDataFrame,Option(schema), false, new JDBCOptions(options+(("dbtable", dst))))
  }
  def getFromOracleTable2(sqlContext: SparkSession): Graph[WholeVertexAttr, WholeEdgeAttr] = {
    val dbstring = options
    import sqlContext.implicits._
    val gd_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_GD"))).load()
    val fddbr_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_FDDBR"))).load()
    val tzf_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_TZF"))).load()
    val trade_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_XFNSR_GFNSR"))).load()

    val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd.
      //            filter(row =>  !(row.getAs[String]("XYGL_XYJB_DM") == "A" && row.getAs[String]("WTBZ")=="Y")).
      //            filter(row =>  row.getAs[String]("XYGL_XYJB_DM") != "A" ).
      map(row =>
      //            if (row.getAs[String]("XYGL_XYJB_DM") == "D")
      //                (row.getAs[BigDecimal]("VERTEXID").longValue(), (40, row.getAs[String]("XYGL_XYJB_DM")))
      //            else
      (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"), row.getAs[String]("WTBZ"))))

    //annotation of david:计算点表
    //unionAll不去重
    val GD_COMPANY_DF = gd_DF.
      filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3")).
      selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL", "GDMC as NAME")
    val TZ_COMPANY_DF = tzf_DF.
      filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3")).
      selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL", "TZFMC as NAME")
    val ZJHM_COMPANY_DF = GD_COMPANY_DF.unionAll(TZ_COMPANY_DF)
    val NSR_VERTEX = ZJHM_COMPANY_DF.selectExpr("TZ_ZJHM as ZJHM").except(fddbr_DF.select("ZJHM")).
      join(ZJHM_COMPANY_DF, $"ZJHM" === $"TZ_ZJHM").
      select("TZ_ZJHM", "NAME").
      rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("TZ_ZJHM"), false))

    val GD_ZZR_DF = gd_DF.filter($"JJXZ".startsWith("5") || $"JJXZ".startsWith("4"))
    val TZ_ZZR_DF = tzf_DF.filter($"TZFXZ".startsWith("5") || $"TZFXZ".startsWith("4"))

    val ZZR_VERTEX = fddbr_DF.selectExpr("ZJHM", "FDDBRMC as NAME").
      unionAll(GD_ZZR_DF.selectExpr("ZJHM", "GDMC as NAME")).
      unionAll(TZ_ZZR_DF.selectExpr("ZJHM", "TZFMC as NAME")).
      rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("ZJHM"), true))

    val maxNsrID = fddbr_DF.agg(max("VERTEXID")).head().getDecimal(0).longValue()

    val ZZR_NSR_VERTEXID = ZZR_VERTEX.union(NSR_VERTEX).
      map { case (name, nsrsbh, ishuman) => (nsrsbh, WholeVertexAttr(name, nsrsbh, ishuman)) }.
      reduceByKey(WholeVertexAttr.combine).zipWithIndex().map { case ((nsrsbh, attr), index) => (index + maxNsrID, attr) }

    val ALL_VERTEX = ZZR_NSR_VERTEXID.
      union(fddbr_DF.
        select("VERTEXID", "NSRDZDAH", "NSRMC").
        rdd.map(row =>
        (row.getAs[BigDecimal]("VERTEXID").longValue(), WholeVertexAttr(row.getAs[String]("NSRMC"), row.getAs[BigDecimal]("NSRDZDAH").toString, false))
      )).
      leftOuterJoin(xyjb).
      map { case (vid, (vattr, opt_fz_dm)) =>
        if (!opt_fz_dm.isEmpty) {
          //                    if(!opt_fz_dm.get._2.equals("A")){
          vattr.xyfz = opt_fz_dm.get._1
          vattr.xydj = opt_fz_dm.get._2
          //                    }
          if (opt_fz_dm.get._3.equals("Y")) vattr.wtbz = true;
        }
        (vid, vattr)
      }.
      persist(StorageLevel.MEMORY_AND_DISK)

    //annotation of david:计算边表

    //annotation of david:特别的，一个ZJHM可能匹配到多个纳税人
    val gd_cc = GD_COMPANY_DF.
      join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = WholeEdgeAttr(0.0, 0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }
    val tz_cc = TZ_COMPANY_DF.
      join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = WholeEdgeAttr(0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0, 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }
    val gd_pc_cc = gd_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(GD_COMPANY_DF.join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.nsrsbh)).
      map { case (sbh1, ((dstid, gdbl), (srcid, attr))) =>
        val eattr = WholeEdgeAttr(0.0, 0.0, gdbl, 0.0)
        ((srcid, dstid), eattr)
      }
    val tz_pc_cc = tzf_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(TZ_COMPANY_DF.join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.nsrsbh)).
      map { case (sbh1, ((dstid, tzbl), (srcid, attr))) =>
        val eattr = WholeEdgeAttr(0.0, tzbl, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    val trade_cc = trade_DF.
      select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
      rdd.map { case row =>
      val eattr = WholeEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
      eattr.se = row.getAs[BigDecimal]("se").doubleValue()
      eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
      eattr.taxrate = row.getAs[BigDecimal]("sl").doubleValue()
      ((row.getAs[BigDecimal]("gf_VERTEXID").longValue(),row.getAs[BigDecimal]("xf_VERTEXID").longValue()), eattr)
    }
    val fddb_pc = fddbr_DF.select("VERTEXID", "ZJHM").
      rdd.map(row => (row.getAs[String](1), row.getAs[BigDecimal](0).longValue())).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.nsrsbh)).
      map { case (sbh1, (dstid, (srcid, attr))) =>
        val eattr = WholeEdgeAttr(1.0, 0.0, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
    val ALL_EDGE = tz_cc.union(gd_cc).union(tz_pc_cc).union(gd_pc_cc).union(trade_cc).union(fddb_pc).
      reduceByKey(WholeEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)
    //annotation of david:获取度大于0的顶点
    // Vertices with no edges are not returned in the resulting RDD.
    val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
    // 使用度大于0的顶点和边构建图
    val ALL_VERTEX_TMP = ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1))
    Graph(ALL_VERTEX_TMP, ALL_EDGE).persist()
  }


  def zipLabel(w_control: Double, w_gd: Double, w_tz: Double, w_trade: Double): String = {
    var toReturn = ""
    val formater = new DecimalFormat("#.###")
    if (w_control > 0) {
      toReturn += "法人；"
    }
    if (w_tz > 0) {
      toReturn += "投资：" + formater.format(w_tz) + "；"
    }
    if (w_gd > 0) {
      toReturn += "控股：" + formater.format(w_gd) + "；"
    }
    if (w_trade > 0) {
      toReturn += "交易：" + formater.format(w_trade) + "；"
    }
    return toReturn.substring(0, toReturn.length - 1)
  }

  def saveWholeEdge(edges: RDD[EdgeTriplet[WholeVertexAttr, WholeEdgeAttr]], sqlContext: SQLContext, dst: String = "WWD_XYCD_EDGE") = {
    DataBaseManager.execute("truncate table " + dst)
    val schema = StructType(
      List(
        StructField("SRC_ID", LongType, true),
        StructField("SRC_NSRDZDAH", StringType, true),
        StructField("DST_ID", LongType, true),
        StructField("DST_NSRDZDAH", StringType, true),
        StructField("CONTROL_WEIGHT", DoubleType, true),
        StructField("GD_WEIGHT", DoubleType, true),
        StructField("INVESTMENT_WEIGHT", DoubleType, true),
        StructField("TRADE_WEIGHT", DoubleType, true),
        StructField("TRADE_JE", DoubleType, true),
        StructField("TRADE_SE", DoubleType, true),
        StructField("LABEL", StringType, true)
      )
    )

    val rowRDD = edges.map(p => Row(p.srcId, p.srcAttr.nsrsbh,
      p.dstId, p.dstAttr.nsrsbh, p.attr.w_control, p.attr.w_gd,
      p.attr.w_tz, p.attr.w_trade, p.attr.trade_je, p.attr.se, zipLabel(p.attr.w_control, p.attr.w_gd,
        p.attr.w_tz, p.attr.w_trade)))
    val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(30)
    JdbcUtils.saveTable(edgeDataFrame,Option(schema), false, new JDBCOptions(options+(("dbtable", dst))))
  }

  def saveFinalScore(finalScore: Graph[ResultVertexAttr, ResultEdgeAttr], sqlContext: SparkSession,
                     edge_dst: String = "WWD_XYCD_EDGE_FINAL", vertex_dst: String = "WWD_XYCD_VERTEX_FINAL", bypass: Boolean = false): Unit = {
    if (!bypass) {
      DataBaseManager.execute("truncate table " + edge_dst)
      val schema = StructType(
        List(
          StructField("source", LongType, true),
          StructField("target", LongType, true),
          StructField("FINAL_INFLUENCE", DoubleType, true)
        )
      )
      val rowRDD = finalScore.edges.map(e => Row(e.srcId, e.dstId, e.attr.influ)).distinct()

      val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(3)
      JdbcUtils.saveTable(edgeDataFrame,Option(schema), false, new JDBCOptions(options+(("dbtable", edge_dst))))
    }

    DataBaseManager.execute("truncate table " + vertex_dst)
    val schema1 = StructType(
      List(
        StructField("VERTICE", LongType, true),
        StructField("INITSCORE", IntegerType, true),
        StructField("FINALSCORE", IntegerType, true),
        StructField("WTBZ", StringType, true)
      )
    )
    val rowRDD1 = finalScore.vertices.map(p => Row(p._1, p._2.old_fz, p._2.new_fz, if (p._2.wtbz) "Y" else "N")).distinct()
    val vertexDataFrame = sqlContext.createDataFrame(rowRDD1, schema1).repartition(3)
    JdbcUtils.saveTable(vertexDataFrame,Option(schema1), false, new JDBCOptions(options+(("dbtable", vertex_dst))))
  }

  def savePath(toOutput: RDD[Seq[(VertexId, String, Double, Double, InfluEdgeAttr)]], sqlContext: SparkSession, edge_dst: String = "WWD_XYCD_EDGE_ML", vertex_dst: String = "WWD_XYCD_VERTEX_ML") = {
    DataBaseManager.execute("truncate table " + edge_dst)
    val schema = StructType(
      List(
        StructField("company", LongType, true),
        StructField("source", LongType, true),
        StructField("target", LongType, true),
        StructField("bel", DoubleType, true),
        StructField("pl", DoubleType, true),
        StructField("label", StringType, true),
        StructField("COMPANY_NSRDZDAH", StringType, true),
        StructField("SRC_NSRDZDAH", StringType, true),
        StructField("DST_NSRDZDAH", StringType, true)
      )
    )
    val rowRDD = toOutput.flatMap { p =>
      val company_id = p.last._1
      val company_nsrdzdah = p.last._2
      val a = Seq[Seq[(VertexId, String, Double, Double, EdgeAttr)]]() ++ p.sliding(2).filter(_.size == 2)
      a.map(e => Row(company_id, e(0)._1, e(1)._1, e(0)._3, e(0)._4, e(0)._5.toString, company_nsrdzdah, e(0)._2, e(1)._2))
    }.distinct()
    val edgeDataFrame = sqlContext.createDataFrame(rowRDD, schema).repartition(3)
    JdbcUtils.saveTable(edgeDataFrame,Option(schema), false,new JDBCOptions(options+(("dbtable", edge_dst))))
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
      flatMap(e => e.map(ee => (e.last._1, e.last._2, ee._1, ee._2))).
      distinct().
      map(p => Row(p._1, p._2, p._3, p._4))
    val vertexDataFrame = sqlContext.createDataFrame(rowRDD1, schema1).repartition(3)
    JdbcUtils.saveTable(vertexDataFrame,Option(schema1), false, new JDBCOptions(options+(("dbtable", vertex_dst))))

  }

}

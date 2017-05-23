package wwd.utils

import _root_.java.math.BigDecimal
import _root_.java.net.URI

import java.Parameters
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import wwd.entity._

import scala.reflect.ClassTag

/**
  * Created by weiwenda on 2017/3/15.
  */
object InputOutputTools {
    def getFromOracleTable2(sqlContext: HiveContext): Graph[WholeVertexAttr, WholeEdgeAttr] = {
        val dbstring = Map(
            "url" -> Parameters.DataBaseURL,
            "user" -> Parameters.DataBaseUserName,
            "password" -> Parameters.DataBaseUserPassword,
            "driver" -> Parameters.JDBCDriverString)

        import sqlContext.implicits._
        val gd_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_GD"))).load()
        val fddbr_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_FDDBR"))).load()
        val tzf_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_TZF"))).load()
        val trade_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_XFNSR_GFNSR"))).load()

        val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring+(("dbtable","XY_NSR_XYJB"))).load()
        val xyjb = XYJB_DF.select("NSRDZDAH", "XYGL_XYJB_DM", "FZ").
            rdd.map(row =>
            if (row.getAs[String]("XYGL_XYJB_DM") == "D")
                (row.getAs[BigDecimal]("NSRDZDAH").toString(), (40, row.getAs[String]("XYGL_XYJB_DM")))
            else
                (row.getAs[BigDecimal]("NSRDZDAH").toString(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"))))

        //annotation of david:计算点表
        //unionAll不去重
        val GD_COMPANY_DF = gd_DF.
            filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3")).
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL","GDMC as NAME")
        val TZ_COMPANY_DF = tzf_DF.
            filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3")).
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL","TZFMC as NAME")
        val ZJHM_COMPANY_DF = GD_COMPANY_DF.unionAll(TZ_COMPANY_DF)
        val NSR_VERTEX = ZJHM_COMPANY_DF.selectExpr("TZ_ZJHM as ZJHM").except(fddbr_DF.select("ZJHM")).
            join(ZJHM_COMPANY_DF,$"ZJHM"===$"TZ_ZJHM").
            select("TZ_ZJHM","NAME").
            rdd.map(row => (row.getAs[String]("NAME"),row.getAs[String]("TZ_ZJHM"), false))

        val GD_ZZR_DF = gd_DF.filter($"JJXZ".startsWith("5") || $"JJXZ".startsWith("4"))
        val TZ_ZZR_DF = tzf_DF.filter($"TZFXZ".startsWith("5") || $"TZFXZ".startsWith("4"))

        val ZZR_VERTEX = fddbr_DF.selectExpr("ZJHM","FDDBRMC as NAME").
            unionAll(GD_ZZR_DF.selectExpr("ZJHM","GDMC as NAME")).
            unionAll(TZ_ZZR_DF.selectExpr("ZJHM","TZFMC as NAME")).
            rdd.map(row => (row.getAs[String]("NAME"),row.getAs[String]("ZJHM"), true))

        val maxNsrID = fddbr_DF.agg(max("VERTEXID")).head().getDecimal(0).longValue()

        val ZZR_NSR_VERTEXID = ZZR_VERTEX.union(NSR_VERTEX).
            map{case(name,nsrsbh,ishuman)=>(nsrsbh,WholeVertexAttr(name, nsrsbh, ishuman))}.
            reduceByKey(WholeVertexAttr.combine).zipWithIndex().map { case ((nsrsbh, attr), index) => (index + maxNsrID, attr) }

        val ALL_VERTEX = ZZR_NSR_VERTEXID.
            union(fddbr_DF.
                select("VERTEXID", "NSRDZDAH","NSRMC").
                rdd.map(row =>
                (row.getAs[BigDecimal]("VERTEXID").longValue(),WholeVertexAttr(row.getAs[String]("NSRMC"),row.getAs[BigDecimal]("NSRDZDAH").toString, false))
            )).
            keyBy(_._2.nsrsbh).leftOuterJoin(xyjb).
            map { case (dzdah, ((vid, vattr), opt_fz_dm)) =>
                if (!opt_fz_dm.isEmpty) {
                    vattr.xyfz = opt_fz_dm.get._1
                    vattr.xydj = opt_fz_dm.get._2
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
                val eattr = WholeEdgeAttr(0.0, 0.0,gdbl, 0.0)
                ((srcid, dstid), eattr)
            }
        val tz_pc_cc = tzf_DF.
            selectExpr("ZJHM", "VERTEXID", "TZBL").
            except(TZ_COMPANY_DF.join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
            rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
            join(ZZR_NSR_VERTEXID.keyBy(_._2.nsrsbh)).
            map { case (sbh1, ((dstid, tzbl), (srcid, attr))) =>
                val eattr = WholeEdgeAttr(0.0,tzbl,0.0, 0.0)
                ((srcid, dstid), eattr)
            }
        val trade_cc = trade_DF.
            select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
            rdd.map { case row =>
            val eattr = WholeEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
            eattr.se = row.getAs[BigDecimal]("se").doubleValue()
            eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
            eattr.taxrate = row.getAs[BigDecimal]("sl").doubleValue()
            ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
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
        Graph(ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1)), ALL_EDGE).persist()
    }
    def getFromOracleTable(sqlContext: HiveContext): Graph[VertexAttr, EdgeAttr] = {

        import sqlContext.implicits._
        val gd_DF = sqlContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_GD",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
        val fddbr_DF = sqlContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_FDDBR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
        val tzf_DF = sqlContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_TZF",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
        val trade_DF = sqlContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_XFNSR_GFNSR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load()

        val XYJB_DF = sqlContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.XY_NSR_XYJB",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load()

        //annotation of david:计算点表,投资人、股东、法人作为点的属性
        val gd_list = gd_DF.
            filter($"JJXZ".startsWith("4") || $"JJXZ".startsWith("5")).
            selectExpr("VERTEXID", "ZJHM as GD", "TZBL").
            rdd.map(row => ((row.getAs[BigDecimal]("VERTEXID").longValue(), row.getAs[String]("GD")), row.getAs[BigDecimal]("TZBL").doubleValue())).
            reduceByKey(_ + _).map { case ((cid, gd), tzbl) => (cid, (gd, tzbl)) }.
            groupByKey()

        val tzf_list = tzf_DF.
            filter($"TZFXZ".startsWith("4") || $"TZFXZ".startsWith("5")).
            selectExpr("VERTEXID", "ZJHM as TZF", "TZBL").
            rdd.map(row => ((row.getAs[BigDecimal]("VERTEXID").longValue(), row.getAs[String]("TZF")), row.getAs[BigDecimal]("TZBL").doubleValue())).
            reduceByKey(_ + _).map { case ((cid, gd), tzbl) => (cid, (gd, tzbl)) }.
            groupByKey()

        val nsr_fddbr = fddbr_DF.selectExpr("VERTEXID", "NSRDZDAH", "ZJHM as FDDBR").
            rdd.map(row => (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("NSRDZDAH").toString, row.getAs[String]("FDDBR"))))
        val xyjb = XYJB_DF.select("NSRDZDAH", "XYGL_XYJB_DM", "FZ").
            rdd.map(row =>
            if (row.getAs[String]("XYGL_XYJB_DM") == "D")
                (row.getAs[BigDecimal]("NSRDZDAH").toString(), (40, row.getAs[String]("XYGL_XYJB_DM")))
            else
                (row.getAs[BigDecimal]("NSRDZDAH").toString(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"))))

        val ALL_VERTEX = nsr_fddbr.leftOuterJoin(tzf_list).keyBy(_._1).leftOuterJoin(gd_list).
            map { case (vid, ((useless, ((nsrdzdah, fddbr), opt_tzflist)), opt_gd_list)) =>
                val vertexAttr = VertexAttr(nsrdzdah, fddbr)
                if (!opt_gd_list.isEmpty)
                    vertexAttr.gd_list = opt_gd_list.get.toSeq
                if (!opt_tzflist.isEmpty)
                    vertexAttr.zrrtz_list = opt_tzflist.get.toSeq
                (vid, vertexAttr)
            }.keyBy(_._2.nsrdzdah).leftOuterJoin(xyjb).
            map { case (dzdah, ((vid, vattr), opt_fz_dm)) =>
                if (!opt_fz_dm.isEmpty) {
                    vattr.xyfz = opt_fz_dm.get._1
                    vattr.xydj = opt_fz_dm.get._2
                }
                (vid, vattr)
            }.
            persist(StorageLevel.MEMORY_AND_DISK)
        //annotation of david:计算边表

        //annotation of david:特别的，一个ZJHM可能匹配到多个纳税人
        val gd_cc1 = gd_DF.
            filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3")).
            filter($"ZJLX_DM" === "10").
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL").
            join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
            select("VERTEXID", "BTZ_VERTEXID", "TZBL")

        val gd_cc2 = gd_DF.
            filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3")).
            filter($"ZJLX_DM" === "90").
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL").
            join(fddbr_DF, $"TZ_ZJHM" === $"NSRSBH").
            select("VERTEXID", "BTZ_VERTEXID", "TZBL")

        val gd_cc = gd_cc1.unionAll(gd_cc2).
            rdd.distinct().map { case row =>
            val eattr = EdgeAttr()
            eattr.kg_bl = row.getAs[BigDecimal]("TZBL").doubleValue()
            ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
        }

        val tzf_cc1 = tzf_DF.
            filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3")).
            filter($"ZJLX_DM" === "10").
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL").
            join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
            select("VERTEXID", "BTZ_VERTEXID", "TZBL")

        val tzf_cc2 = tzf_DF.
            filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3")).
            filter($"ZJLX_DM" === "90").
            selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL").
            join(fddbr_DF, $"TZ_ZJHM" === $"NSRSBH").
            select("VERTEXID", "BTZ_VERTEXID", "TZBL")


        val tz_cc = tzf_cc1.unionAll(tzf_cc2).
            rdd.distinct().map { case row =>
            val eattr = EdgeAttr()
            eattr.tz_bl = row.getAs[BigDecimal]("TZBL").doubleValue()
            ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
        }

        val trade_cc = trade_DF.
            select("xf_VERTEXID", "gf_VERTEXID", "jybl").
            rdd.map { case row =>
            val eattr = EdgeAttr()
            eattr.jy_bl = row.getAs[BigDecimal]("jybl").doubleValue()
            ((row.getAs[BigDecimal]("xf_VERTEXID").longValue(), row.getAs[BigDecimal]("gf_VERTEXID").longValue()), eattr)
        }

        // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
        val ALL_EDGE = tz_cc.union(trade_cc).union(gd_cc).
            reduceByKey(EdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
            map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
            persist(StorageLevel.MEMORY_AND_DISK)
        //annotation of david:获取度大于0的顶点
        // Vertices with no edges are not returned in the resulting RDD.
        val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
        // 使用度大于0的顶点和边构建图
        Graph(ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1)), ALL_EDGE).persist()
    }

    // 保存TPIN到HDFS
    def saveAsObjectFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                                 verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd"): Unit = {

        checkDirExist(sparkContext, verticesFilePath)
        checkDirExist(sparkContext, edgesFilePath)
        // 对象方式保存顶点集
        tpin.vertices.repartition(1).saveAsObjectFile(verticesFilePath)
        // 对象方式保存边集
        tpin.edges.repartition(1).saveAsObjectFile(edgesFilePath)
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
        val hdfs = FileSystem.get(new URI("hdfs://cloud-03:9000"), sc.hadoopConfiguration)
        try {
            hdfs.delete(new Path(outpath), true)
        }
        catch {
            case e: Throwable => e.printStackTrace()
        }
    }

    // 从HDFS获取TPIN
    def getFromObjectFile[VD: ClassTag, ED: ClassTag](sparkContext: SparkContext, verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd")
    : Graph[VD, ED] = {
        // 对象方式获取顶点集
        val vertices = sparkContext.objectFile[(VertexId, VD)](verticesFilePath).repartition(200)
        // 对象方式获取边集
        val edges = sparkContext.objectFile[Edge[ED]](edgesFilePath).repartition(200)
        // 构建图
        Graph[VD, ED](vertices, edges)
    }

    def getFromCsv(sc: SparkContext, vertexPath: String, edgePath: String): Graph[VertexAttr, EdgeAttr] = {
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
                val vertex = VertexAttr(node(1), node(2))
                vertex.gd_list = gd_list
                vertex.zrrtz_list = zrrtz_list
                vertex.xyfz = node.last.toInt
                (node(0).toLong, vertex)
        }

        val edges = edgesTxt.filter(!_.startsWith("source")).map(_.split(",")).map {
            case e =>
                val eattr = EdgeAttr()
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
        val hdfs = FileSystem.get(new URI("hdfs://cloud-03:9000"), sc.hadoopConfiguration)
        hdfs.exists(new Path(outpath))
    }
}

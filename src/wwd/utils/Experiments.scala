package wwd.utils

import java.math.BigDecimal
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import wwd.entity.{InfluenceEdgeAttr, EdgeAttr, VertexAttr}

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {
    //annotation of david:输出边的影响值和点的修正前后分值到Oracle
    def collect_influence2(sc:SparkContext,hiveContext: HiveContext) ={

        //annotation of david: verticesAfter: RDD[(graphx.VertexId, Int)] vertexid xyfx_after
        val verticesAfter = sc.objectFile[(VertexId, Int)]("/tpin/wwd/influence/fixed_vertices").repartition(200)

        //annotation of david: verticesBefore: RDD[(graphx.VertexId, Int)] vertexid,xyfz_before
        val verticesBefore = sc.objectFile[(VertexId, VertexAttr)]("/tpin/wwd/influence/vertices").repartition(200).map(e => (e._1, e._2.xyfz))

        //annotation of david: output : RDD[(Long, Int, Int,Boolean)] vertexid,xyfx_after,xyfz_before
        val output = verticesAfter.join(verticesBefore).map { case (vid, xyfz) =>
            (vid,( xyfz._1, xyfz._2))
        }
        val nsrdzdahInfo = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_FDDBR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().select("NSRDZDAH", "VERTEXID").
            rdd.repartition(120).map(row => ((row.getAs[BigDecimal]("VERTEXID").longValue(),row.getAs[BigDecimal]("NSRDZDAH").toString)))
        val vertices = output.join(nsrdzdahInfo).map{case(vid,((after,before),nsrdzdah))=>
            (vid,(after,before,nsrdzdah))
        }
        val edges = sc.objectFile[Edge[Double]]("/tpin/wwd/influence/fixed_edges").repartition(200)

        val tpin = Graph[((Int, Int, String)), Double](vertices, edges).mapTriplets(triplet=>
            (triplet.dstAttr._3,triplet.srcAttr._3,triplet.attr,triplet.srcAttr._2.toDouble,triplet.srcAttr._1.toDouble)
        )
        OracleDBUtil.saveAllVertex(tpin.edges.map(_.attr),hiveContext)
        OracleDBUtil.saveAllEdge(tpin.edges.map(e=>(e.attr._1,e.attr._2,e.attr._1,e.attr._3)),hiveContext)
    }

    //annotation of david:输出边的影响值和点的修正前后分值到文本文件
    def collect_influence(sc: SparkContext, hiveContext: HiveContext) = {
        val tpin = InputOutputTools.getFromObjectFile[VertexAttr, EdgeAttr](sc, "/tpin/wwd/influence/vertices", "/tpin/wwd/influence/edges")
        val toOutput = MessagePropagation.
            fixVertexWeight(tpin).
            mapTriplets { case triplet =>
                val controllerInterSect = MessagePropagation.computeCI(triplet.srcAttr, triplet.dstAttr, kind = 2)
                s"fr:$controllerInterSect tz:${triplet.attr.tz_bl} kg:${triplet.attr.kg_bl} jy:${triplet.attr.jy_bl}"
            }.
            outerJoinVertices(collect_xyfz_wtbz(sc, hiveContext, "/tpin/wwd/influence/fixed_vertices")) {
                case (vid, vattr, opt) => if (!opt.isEmpty) (opt.get._1, opt.get._2, vattr.xydj, opt.get._3) else (0, 0, vattr.xydj, "no")
            }.

        //annotation of david:只关注被选案的企业
            subgraph(vpred = (vid, vattr) => vattr._1 > 0 && vattr._4 !="no")
        //epred = triplet => triplet.attr.productIterator.map(_.asInstanceOf[Double]).max > 0.1
        InputOutputTools.saveAsTextFile(toOutput, sc, verticesFilePath = "/tpin/wwd/influence/vertices_toshow", edgesFilePath = "/tpin/wwd/influence/edges_toshow")
    }


    //annotation of david:输出点的修正前后分值和命中情况到Oracle
    def collect_xyfz_wtbz(sc: SparkContext, hiveContext: HiveContext, verticesFilePath: String, beforePath: String = "/tpin/wwd/influence/vertices", toOracle: Boolean = false) = {

        //annotation of david: badList: RDD[(Long, String)] wt_vertexid,nsrdzdah
        val badList = getTroubleList(hiveContext)

        //annotation of david: verticesAfter: RDD[(graphx.VertexId, Int)] vertexid xyfx_after
        val verticesAfter = sc.objectFile[(VertexId, Int)](verticesFilePath).repartition(200).
            filter(_._2 > 0).
            sortBy(e => e._2)

        //annotation of david: verticesBefore: RDD[(graphx.VertexId, Int)] vertexid,xyfz_before
        val verticesBefore = sc.objectFile[(VertexId, VertexAttr)](beforePath).repartition(200).
            //            filter(_._2.xyfz>0).
            map(e => (e._1, e._2.xyfz)).
            sortBy(_._2)

        //annotation of david: output : RDD[(Long, Int, Int,Boolean)] vertexid,xyfx_after,xyfz_before,wtbz
        val output = verticesAfter.join(verticesBefore).leftOuterJoin(badList).map { case (vid, (xyfz, opt)) =>
            if (opt.isEmpty) (vid, xyfz._1, xyfz._2, "no","")
            else if(opt.get._1)(vid, xyfz._1, xyfz._2, "false",opt.get._2)
            else (vid,xyfz._1,xyfz._2,"true",opt.get._2)
        }
        if (toOracle) {
            DataBaseManager.execute("truncate table WWD_INFLUENCE_RESULT")
            OracleDBUtil.saveAsOracleTable(output, hiveContext)
        }
        println("vertices prepared!")
        output.map { e => (e._1, (e._2, e._3, e._4)) }
    }

    //annotation of david: RDD[(Long, Boolean)] 布尔代表选案结果
    def getTroubleList(hiveContext: HiveContext) = {

        import hiveContext.implicits._
        val all = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.JC_AJXX",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().
            //            filter($"AJLY_DM".isin(20,92,40)).
            withColumn("wtbz",$"WFWZLX_DM".isNull).
            select("NSRDZDAH", "LRRQ","wtbz","AJLY_DM").rdd.
            filter(row => row.getAs[Date]("LRRQ").getYear() + 1900 >= 2014 && row.getAs[Date]("LRRQ").getYear() + 1900 <= 2015).
            map { row => (row.getAs[BigDecimal]("NSRDZDAH").toString, (row.getAs[Boolean]("wtbz"),row.getAs[String]("AJLY_DM"))) }.distinct()
        val nsrdzdahInfo = hiveContext.read.format("jdbc").options(
            Map("url" -> "jdbc:oracle:thin:@202.117.16.32:1521:shannxi",
                "dbtable" -> "shannxi2016.WWD_NSR_FDDBR",
                "user" -> "shannxi",
                "password" -> "shannxi",
                "driver" -> "oracle.jdbc.driver.OracleDriver")).load().select("NSRDZDAH", "VERTEXID").
            rdd.repartition(120).map(row => ((row.getAs[BigDecimal]("NSRDZDAH").toString, (row.getAs[BigDecimal]("VERTEXID").longValue()))))

        //annotation of david:按纳税人档案号 得到 顶点编号
        val all_vid = nsrdzdahInfo.join(all).map { case (nsrdzdah, (vid, ajxx)) => (vid, ajxx) }
        all_vid
    }

}

package wwd.utils

import _root_.java.lang.reflect.Parameter
import _root_.java.math.BigDecimal
import _root_.java.text.SimpleDateFormat
import _root_.java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import wwd.entity._
import wwd.utils.java.{CSVProcess, Parameters, DataBaseManager}

import scala.Range
import scala.util.Random

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {

    //annotation of david:修改wtbz based on the Neighbor situation
    def prepareGroundTruth(graph: Graph[VertexAttr, EdgeAttr]) = {
        val msg = graph.aggregateMessages[Seq[Int]](ctx => {
            if (ctx.dstAttr.xyfz > 0)
                ctx.sendToSrc(Seq((ctx.dstAttr.xyfz)))
            if (ctx.srcAttr.xyfz > 0)
                ctx.sendToDst(Seq((ctx.srcAttr.xyfz)))
        }, _ ++ _)
        graph.outerJoinVertices(msg) { case (vid, old, opt) =>
            if (!opt.isEmpty) {
                val avg = opt.get.sum / opt.get.size
                if ((old.xyfz > 0 && old.xyfz < 60) || (avg < 80 && old.xyfz > 60))
                    if (Random.nextFloat() < 0.8) old.wtbz = true
                old
            } else {
                old
            }
        }.cache()
    }

    def computeAUC(graph: Graph[(Int, Int, Boolean), Double], total_num: Int = 1000, new_old: String = "new") = {
        var right: Array[Int] = null
        var wrong: Array[Int] = null
        if (new_old.equals("new")) {
            right = graph.vertices.filter(e => !e._2._3 && e._2._1 > 0).map(e => (e._2._2)).takeSample(true, total_num)
            wrong = graph.vertices.filter(e => e._2._3 && e._2._1 > 0).map(e => (e._2._2)).takeSample(true, total_num)
        } else {
            right = graph.vertices.filter(e => !e._2._3 && e._2._1 > 0).map(e => (e._2._1)).takeSample(true, total_num)
            wrong = graph.vertices.filter(e => e._2._3 && e._2._1 > 0).map(e => (e._2._1)).takeSample(true, total_num)
        }
        var score: Double = 0D
        for (i <- Range(0, total_num)) {
            if (right(i) > wrong(i)) score += 1
            else if (right(i) == wrong(i)) score += 0.5
        }
        score / total_num
    }

    //annotation of david:异常企业的邻居低分率
    def computeNeighbor(graph: Graph[(Int, Int, Boolean), Double]) = {
        val msg = graph.subgraph(epred = ctx => ctx.srcAttr._3 == true || ctx.dstAttr._3 == true).
            aggregateMessages[Seq[Int]](ctx => {
            if (ctx.srcAttr._3 == true)
                ctx.sendToSrc(Seq((ctx.dstAttr._1)))
            if (ctx.dstAttr._3 == true)
                ctx.sendToDst(Seq((ctx.srcAttr._1)))
        }, _ ++ _)
        val pref = graph.vertices.join(msg).filter { case (vid, ((oldfz, newfz, wtbz), msg)) =>
            msg.filter(e => e <= 40 && e > 0).size > 0
        }.count()
        //        val count = graph.vertices.filter(_._2._3).count()
        val count = graph.vertices.join(msg).count()
        pref / count.toDouble
    }

    //annotation ofw david:异常企业的分数变化趋势
    def computeTendency(graph: Graph[(Int, Int, Boolean), Double]) = {
        val co1 = graph.vertices.filter { case (vid, (old, newfz, wtbz)) => wtbz && newfz <= old }.count()
        val co2 = graph.vertices.filter(_._2._3).count()
        co1 / co2.toDouble
    }

    //annotation of david:邻居误检率
    def computePref_new(graph: Graph[(Int, Int, Boolean), Double]) = {
        val msg = graph.subgraph(epred = ctx => ctx.srcAttr._3 == true || ctx.dstAttr._3 == true).
            aggregateMessages[Seq[(Int, Boolean)]](ctx => {
            if (ctx.srcAttr._3 == true)
                ctx.sendToSrc(Seq((ctx.dstAttr._2, ctx.dstAttr._3)))
            if (ctx.dstAttr._3 == true)
                ctx.sendToDst(Seq((ctx.srcAttr._2, ctx.srcAttr._3)))
        }, _ ++ _)
        val pref = graph.vertices.join(msg).map { case (vid, ((oldfz, newfz, wtbz), msg)) =>
            val count = msg.filter(_._1 < newfz).size
            val mz = msg.filter { case (fz, wtbzo) =>
                wtbzo == false && fz < newfz //&& fz!=0
            }.size
            if (count == 0)
                0
            else
                mz / count.toDouble
        }
        val prefinal = pref.reduce(_ + _) / pref.count()
        prefinal
    }

    def computePref_old(graph: Graph[(Int, Int, Boolean), Double]) = {
        val msg = graph.subgraph(epred = ctx => ctx.srcAttr._3 == true || ctx.dstAttr._3 == true).
            aggregateMessages[Seq[(Int, Boolean)]](ctx => {
            if (ctx.srcAttr._3 == true)
                ctx.sendToSrc(Seq((ctx.dstAttr._1, ctx.dstAttr._3)))
            if (ctx.dstAttr._3 == true)
                ctx.sendToDst(Seq((ctx.srcAttr._1, ctx.srcAttr._3)))
        }, _ ++ _)
        val pref = graph.vertices.join(msg).map { case (vid, ((oldfz, newfz, wtbz), msg)) =>
            val count = msg.filter(_._1 < oldfz).size
            val mz = msg.filter { case (fz, wtbzo) =>
                wtbzo == false && fz < oldfz // && fz!=0
            }.size
            if (count == 0)
                0
            else
                mz / count.toDouble
        }
        val prefinal = pref.reduce(_ + _) / pref.count()
        prefinal

    }

    def computeRankScore(graph: Graph[(Int, Int, Boolean), Double]) ={
        val sorted = graph.vertices.filter(_._2._1>0).sortBy(_._2._2).zipWithIndex()
        val num1 = sorted.count()
        val sorted1 = sorted.filter(e=>e._1._2._3).map(e=>e._2/num1.toDouble)
        val num2 = sorted1.count()
        sorted1.sum()/num2
    }
    //annotation of david:对比有无互锁边的区别
    def Experiment_3(sc: SparkContext, hiveContext: HiveContext) = {
        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        val fw1 = CSVProcess.openCsvFileHandle(s"Result/experiment_3.csv", true)
        CSVProcess.saveAsCsvFile(fw1, "alpha,AUC,RankScore,误检率,变化趋势")
        val tpin = InputOutputTools.getFromObjectFile[VertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        val influenceGraph = MessagePropagation.runFuzzWithoutIL(tpin,hiveContext,bypass=true,lambda = 3).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
        InputOutputTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        val influenceGraph1 = InputOutputTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        for (index <- Range(0, 21)) {
            println(s"\r开始=> alpha:${index}  timestamp:${format.format(new Date())}")

            val finalScore = CombineNSXY.run(influenceGraph1,index*0.05).cache()
            CSVProcess.saveAsCsvFile(fw1, s"${index},${computeAUC(finalScore,total_num = 3000)},${computeRankScore(finalScore)},${computePref_new(finalScore)},${computeTendency(finalScore)}")
            finalScore.unpersistVertices(blocking = false)
            finalScore.edges.unpersist(blocking = false)
        }
        CSVProcess.closeCsvFileHandle(fw1)
        println("Finished!")
    }

    def Experiment_2(sc: SparkContext, hiveContext: HiveContext) = {
        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        val fw1 = CSVProcess.openCsvFileHandle(s"Result/experiment_2.csv", true)
        CSVProcess.saveAsCsvFile(fw1, "alpha,AUC,RankScore,误检率,变化趋势")
        val tpin = InputOutputTools.getFromObjectFile[VertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        val influenceGraph = MessagePropagation.runFuzz(tpin,hiveContext,bypass=true,lambda = 3).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
        InputOutputTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        val influenceGraph1 = InputOutputTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        for (index <- Range(0, 21)) {
            println(s"\r开始=> alpha:${index}  timestamp:${format.format(new Date())}")

            val finalScore = CombineNSXY.run(influenceGraph1,index*0.05).cache()
            CSVProcess.saveAsCsvFile(fw1, s"${index},${computeAUC(finalScore,total_num = 3000)},${computeRankScore(finalScore)},${computePref_new(finalScore)},${computeTendency(finalScore)}")
            finalScore.unpersistVertices(blocking = false)
            finalScore.edges.unpersist(blocking = false)
        }
        CSVProcess.closeCsvFileHandle(fw1)
        println("Finished!")

    }
    def Experiment_1(sc: SparkContext, hiveContext: HiveContext,out :Int = 0 ,in:Int =0) = {

        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        val fw1 = CSVProcess.openCsvFileHandle(s"Result/experiment_1.csv", true)
        CSVProcess.saveAsCsvFile(fw1, "影响力计算方法,传递影响计算方法,AUC,RankScore,误检率,变化趋势")
        val method1s = Array("maxmin","proba","bouned","ds","TidalTrust","fuzz")
        val method2s = Array("min","product","(a * b) / (a + b - a * b)","a + b - 1")
        val tpin = InputOutputTools.getFromObjectFile[VertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        for (method1 <- Range(0, 6)) {
            for (method2 <- Range(0, 4)) {
                if(method1 < out || (method1== out &&method2 < in)) {

                }else{
                    var influenceGraph: Graph[(Int, Boolean), Double] = null

                    println(s"\r开始=> 影响力计算方法:${method1s(method1)} 传递影响计算方法:${method2s(method2)} timestamp:${format.format(new Date())}")

                    method1 match {
                        case 0 =>
                            influenceGraph = MessagePropagation.run(tpin,hiveContext,bypass=true,method=method1s(method1),lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                        case 1 =>
                            influenceGraph = MessagePropagation.run(tpin,hiveContext,bypass=true,method=method1s(method1),lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                        case 2 =>
                            influenceGraph = MessagePropagation.run(tpin,hiveContext,bypass=true,method=method1s(method1),lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                        case 3 =>
                            influenceGraph = MessagePropagation.run(tpin,hiveContext,bypass=true,method=method1s(method1),lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                        case 4 =>
                            influenceGraph = MessagePropagation.runTidalTrust(tpin,hiveContext,lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                        case 5 =>
                            influenceGraph = MessagePropagation.runFuzz(tpin,hiveContext,bypass=true,lambda = method2).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
                    }

                    val finalScore = CombineNSXY.run(influenceGraph).cache()

                    CSVProcess.saveAsCsvFile(fw1, s"${method1s(method1)},${method2s(method2)},${computeAUC(finalScore)},${computeRankScore(finalScore)},${computePref_new(finalScore)},${computeTendency(finalScore)}")

                    influenceGraph.unpersistVertices(blocking = false)
                    influenceGraph.edges.unpersist(blocking = false)
                    finalScore.unpersistVertices(blocking = false)
                    finalScore.edges.unpersist(blocking = false)
                }
            }
        }
        CSVProcess.closeCsvFileHandle(fw1)
        println("Finished!")
    }
}

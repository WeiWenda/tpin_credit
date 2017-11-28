package wwd.main

import _root_.java.text.SimpleDateFormat
import _root_.java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import wwd.entity._
import wwd.entity.impl.InfluVertexAttr
import wwd.utils.{CSVProcess, HdfsTools}

import scala.util.Random

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {

    //annotation of david:修改wtbz based on the Neighbor situation
    def prepareGroundTruth(graph: Graph[InfluVertexAttr, EdgeAttr]) = {
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
    //annotation of david:对比有无互锁边的区别
    def Experiment_3(sc: SparkContext, hiveContext: HiveContext) = {
        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        val fw1 = CSVProcess.openCsvFileHandle(s"Result/experiment_3.csv", true)
        CSVProcess.saveAsCsvFile(fw1, "alpha,AUC,RankScore,误检率,变化趋势")
        val tpin = HdfsTools.getFromObjectFile[InfluVertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        val influenceGraph = MessagePropagation.runFuzzWithoutIL(tpin,hiveContext,bypass=true,lambda = 3).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
        HdfsTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        val influenceGraph1 = HdfsTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
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
        val tpin = HdfsTools.getFromObjectFile[InfluVertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
        val influenceGraph = MessagePropagation.runFuzz(tpin,hiveContext,bypass=true,lambda = 3).mapVertices((vid,vattr)=>(vattr.xyfz,vattr.wtbz)).cache()
        HdfsTools.saveAsObjectFile(influenceGraph,sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
        val influenceGraph1 = HdfsTools.getFromObjectFile[(Int,Boolean),Double](sc,"/tpin/wwd/influence/inf_vertices_FuzzyTrust","/tpin/wwd/influence/inf_edges_FuzzyTrust")
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
        val tpin = HdfsTools.getFromObjectFile[InfluVertexAttr,EdgeAttr](sc,"/tpin/wwd/influence/vertices","/tpin/wwd/influence/edges")
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

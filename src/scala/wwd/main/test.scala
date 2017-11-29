package wwd.main

import java.util.Date

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import wwd.utils.HdfsTools

import scala.Seq
import scala.reflect.ClassTag
import org.apache.spark.graphx._

/**
  * Created by weiwenda on 2017/7/13.
  */
object test {
    type Path = Seq[VertexId]
    type Paths = Seq[Seq[VertexId]]
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
        val vertices = sc.parallelize(Seq((1,"vertex1"),(2,"vertex2"),(3,"vertex3"),(4,"vertex4"),(5,"vertex5"),(6,"vertex6"))).
            map(v=>(v._1.toLong,Seq(Seq(v._1.toLong))))
        val v1 = vertices.map(e=>(e._1,new Date()))
        val v2 = v1.map(e=>(e._1,(e._2,"again")))
        val v3 = v1.join(v2)
        println(v3.collect())
//        val edges = sc.parallelize(Seq((1,2,"edge1"),(2,3,"edge2"),(3,4,"edge3"),(4,5,"edge4"))).map(e=>Edge(e._1,e._2,e._3))
//        val graph= Graph(vertices,edges)
//        val result = getPath(graph,4,1)
//        InputOutputTools.printGraph(result)
    }

    //annotation of david:收集所有长度initlength-1到maxIteration-1的路径
    def getPath(graph: Graph[Paths, String], maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
        // 发送路径
        def sendPaths(edge: EdgeContext[Paths, String, Paths],
                      length: Int): Unit = {
            val satisfied = edge.dstAttr.filter(e => e.size == length)
            if (satisfied.size > 0) {
                // 向终点发送顶点路径集合
                edge.sendToSrc(satisfied.map(Seq(edge.srcId) ++ _))
            }
        }
        var preproccessedGraph = graph.cache()
        var i = initLength
        var messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _)
        var activeMessages = messages.count()
        var prevG: Graph[Paths, String] = null
        while (activeMessages > 0 && i <= maxIteratons) {
            prevG = preproccessedGraph
            preproccessedGraph = preproccessedGraph.joinVertices[Paths](messages)((id, vd, path) => vd ++ path).cache()
            print("iterator " + i + " finished! ")
            i += 1
            val oldMessages = messages
            messages = preproccessedGraph.aggregateMessages[Paths](sendPaths(_, i), _ ++ _).cache()
            try{
                activeMessages = messages.count()
            }catch{
                case ex:Exception =>
                    println("又发生异常了")
            }
            oldMessages.unpersist(blocking = false)
            prevG.unpersistVertices(blocking = false)
            prevG.edges.unpersist(blocking = false)
        }
        //         printGraph[Paths,Int](preproccessedGraph)
        preproccessedGraph
    }

}

package wwd.utils

import _root_.java.lang.reflect.Parameter
import _root_.java.math.BigDecimal
import _root_.java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import wwd.entity.{InfluenceEdgeAttr, EdgeAttr, VertexAttr}
import wwd.utils.java.{Parameters, DataBaseManager}

/**
  * Created by weiwenda on 2017/3/20.
  */
object Experiments {
    def computePref_new(graph: Graph[(Int, Int, Boolean), Double]) = {
        val msg = graph.subgraph(epred = ctx=>ctx.srcAttr._3 == true||ctx.dstAttr._3 == true).
            aggregateMessages[Seq[(Int,Boolean)]](ctx=> {
                if(ctx.srcAttr._3==true)
                    ctx.sendToSrc(Seq((ctx.dstAttr._2,ctx.dstAttr._3)))
                if(ctx.dstAttr._3==true)
                    ctx.sendToDst(Seq((ctx.srcAttr._2,ctx.srcAttr._3)))
            },_++_)
        val pref = graph.vertices.join(msg).map{case(vid,((oldfz,newfz,wtbz),msg))=>
                val count = msg.size
                val mz = msg.filter{case(fz,wtbzo)=>
                        wtbzo == false && fz<newfz
                }.size
                mz/count.toDouble
        }
        val prefinal = pref.reduce(_+_)/pref.count()
        prefinal
    }

    def computePref_old(graph: Graph[(Int, Int, Boolean), Double]) = {
        val msg = graph.subgraph(epred = ctx=>ctx.srcAttr._3 == true||ctx.dstAttr._3 == true).
            aggregateMessages[Seq[(Int,Boolean)]](ctx=> {
            if(ctx.srcAttr._3==true)
                ctx.sendToSrc(Seq((ctx.dstAttr._1,ctx.dstAttr._3)))
            if(ctx.dstAttr._3==true)
                ctx.sendToDst(Seq((ctx.srcAttr._1,ctx.srcAttr._3)))
        },_++_)
        val pref = graph.vertices.join(msg).map{case(vid,((oldfz,newfz,wtbz),msg))=>
            val count = msg.size
            val mz = msg.filter{case(fz,wtbzo)=>
                wtbzo == false && fz<oldfz
            }.size
            mz/count.toDouble
        }
        val prefinal = pref.reduce(_+_)/pref.count()
        prefinal

    }
}

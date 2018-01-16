package wwd.evaluation

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import wwd.strategy.impl.{ResultEdgeAttr, ResultVertexAttr}

import scala.reflect.ClassTag

abstract class EvaluateResult[T: ClassTag] {
  def toString(separator: String): String
}

case class TwoResult[T: ClassTag](before: T, after: T) extends EvaluateResult[T] {
  override def toString(separator: String): String = {
    before + separator + after
  }
}

case class OneResult[T: ClassTag](one: T) extends EvaluateResult[T] {
  override def toString(separator: String): String = {
    one.toString
  }
}

abstract class Measurement[VD: ClassTag, ED: ClassTag, RD: ClassTag] {
  def compute(graph: Graph[VD, ED]): RD
}

/**
  * Author: weiwenda
  * Description: 异常企业的邻居低分率,返回结果为两个Double，第一个为修改前，第二个为修改后
  * 1.观察问题企业周边所有企业
  * 2.周围一圈有D级纳税人 / 全部问题纳税人
  * Date: 下午4:09 2017/11/28
  */
case class NEIGHBOR() extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {
  def computeNEIGHBOR(graph: Graph[ResultVertexAttr, ResultEdgeAttr], switcher: String): Double = {
    var msg: VertexRDD[scala.Seq[Int]] = null
    switcher match {
      case "old" =>
        msg = graph.subgraph(epred = ctx => {
          ctx.srcAttr.wtbz == true || ctx.dstAttr.wtbz == true
        }).
          aggregateMessages[Seq[Int]](ctx => {
          if (ctx.srcAttr.wtbz == true)
            ctx.sendToSrc(Seq((ctx.dstAttr.old_fz)))
          if (ctx.dstAttr.wtbz == true)
            ctx.sendToDst(Seq((ctx.srcAttr.old_fz)))
        }, _ ++ _)
      case "new" =>
        msg = graph.subgraph(epred = ctx => {
          ctx.srcAttr.wtbz == true || ctx.dstAttr.wtbz == true
        }).
          aggregateMessages[Seq[Int]](ctx => {
          if (ctx.srcAttr.wtbz == true)
            ctx.sendToSrc(Seq((ctx.dstAttr.new_fz)))
          if (ctx.dstAttr.wtbz == true)
            ctx.sendToDst(Seq((ctx.srcAttr.new_fz)))
        }, _ ++ _)
    }
    val pref = graph.vertices.join(msg).filter { case (vid, (vattr, msg)) =>
      msg.filter(e => e <= 40 && e > 0).size > 0
    }.count()
    //        val count = graph.vertices.filter(_._2._3).count()
    val count = graph.vertices.join(msg).count()
    pref / count.toDouble
  }

  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): EvaluateResult[Double] = {
    TwoResult(computeNEIGHBOR(graph, "old"), computeNEIGHBOR(graph, "new"))
  }

}
/**
 *Author:weiwenda
 *Description:计算KS
 *Date:22:57 2017/12/26
 */
case class KS()extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {

  def computeKS(before: Seq[(Double, Double)]):Double = {
    before.map(e=>e._2-e._1).max
  }
  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): EvaluateResult[Double] = {
    val roc = ROC().compute(graph)
    TwoResult(computeKS(roc.before),computeKS(roc.after))
  }
}
/**
  * Author:weiwenda
  * Description:绘制Roc曲线
  * Date:22:32 2017/12/26
  */
case class ROC() extends Measurement[ResultVertexAttr, ResultEdgeAttr, TwoResult[Seq[(Double,Double)]]] {
  def computeROC(graph: Graph[ResultVertexAttr, ResultEdgeAttr], switcher: String) = {
    var collection: RDD[(VertexId, (ResultVertexAttr, Long))] = null
    switcher match {
      case "old" =>
        collection = graph.vertices.filter(_._2.old_fz > 0).sortBy(_._2.old_fz).zipWithIndex().
          map { case ((vid, vattr), index) => (vid, (vattr, index)) }
      case "new" =>
        collection = graph.vertices.filter(_._2.old_fz > 0).sortBy(_._2.new_fz).zipWithIndex().
          map { case ((vid, vattr), index) => (vid, (vattr, index)) }
    }
    val granularity = collection.count / 100 + 1
    val focus = collection.filter(_._2._1.wtbz).count.toDouble
    val nonfocus = collection.filter(!_._2._1.wtbz).count.toDouble
    Range(1, 101).
      map { case index =>
        val tpr = collection.filter(e => e._2._2 < index * granularity && e._2._1.wtbz).count / focus
        val fpr = collection.filter(e => e._2._2 < index * granularity && !e._2._1.wtbz).count / nonfocus
        (fpr, tpr)
      }
  }
  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): TwoResult[Seq[(Double,Double)]] = {
    TwoResult(computeROC(graph, "old"), computeROC(graph, "new"))
  }
}
/**
  * Author:weiwenda
  * Description:绘制PR曲线
  * Date:22:32 2017/12/26
  */
case class PR() extends Measurement[ResultVertexAttr, ResultEdgeAttr, TwoResult[Seq[(Double,Double)]]] {
  def computePR(graph: Graph[ResultVertexAttr, ResultEdgeAttr], switcher: String) = {
    var collection: RDD[(VertexId, (ResultVertexAttr, Long))] = null
    switcher match {
      case "old" =>
        collection = graph.vertices.filter(_._2.old_fz > 0).sortBy(_._2.old_fz).zipWithIndex().
          map { case ((vid, vattr), index) => (vid, (vattr, index)) }
      case "new" =>
        collection = graph.vertices.filter(_._2.old_fz > 0).sortBy(_._2.new_fz).zipWithIndex().
          map { case ((vid, vattr), index) => (vid, (vattr, index)) }
    }
    val granularity = collection.count / 100 + 1
    val focus = collection.filter(_._2._1.wtbz).count.toDouble
    val nonfocus = collection.filter(!_._2._1.wtbz).count.toDouble
    Range(1, 101).
      map { case index =>
        val recall = collection.filter(e => e._2._2 < index * granularity && e._2._1.wtbz).count / focus
        val precise = collection.filter(e => e._2._2 < index * granularity && e._2._1.wtbz).count /
          collection.filter(e => e._2._2 < index * granularity).count.toDouble
        (recall,precise)
      }
  }
  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): TwoResult[Seq[(Double,Double)]] = {
    TwoResult(computePR(graph, "old"), computePR(graph, "new"))
  }
}

/**
  * Author: weiwenda
  * Description: 计算AUC值，返回结果为两个Double，第一个为修改前，第二个为修改后
  * Date: 下午4:09 2017/11/28
  */
case class AUC(val total_num: Int = 1000) extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {
  def computeAUC(right: Array[Int], wrong: Array[Int]): Double = {
    var score: Double = 0D
    for (i <- Range(0, total_num)) {
      if (right(i) > wrong(i)) score += 1
      else if (right(i) == wrong(i)) score += 0.5
    }
    score / total_num
  }

  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): EvaluateResult[Double] = {
    val right_new = graph.vertices.filter(e => !e._2.wtbz && e._2.old_fz > 0).map(e => (e._2.new_fz)).takeSample(true, total_num)
    val wrong_new = graph.vertices.filter(e => e._2.wtbz && e._2.old_fz > 0).map(e => (e._2.new_fz)).takeSample(true, total_num)
    val right_old = graph.vertices.filter(e => !e._2.wtbz && e._2.old_fz > 0).map(e => (e._2.old_fz)).takeSample(true, total_num)
    val wrong_old = graph.vertices.filter(e => e._2.wtbz && e._2.old_fz > 0).map(e => (e._2.old_fz)).takeSample(true, total_num)
    TwoResult(computeAUC(right_old, wrong_old), computeAUC(right_new, wrong_new))
  }
}

/**
  * Author: weiwenda
  * Description: 邻居误检率，分数低于中心企业，但没有wtbz的企业比例
  * Date: 下午8:53 2017/11/28
  */
case class PREF() extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {
  def computePREF(graph: Graph[ResultVertexAttr, ResultEdgeAttr], switcher: String) = {
    var msg: VertexRDD[scala.Seq[(Int, Boolean)]] = null
    switcher match {
      case "old" =>
        msg = graph.subgraph(epred = ctx => ctx.srcAttr.wtbz == true || ctx.dstAttr.wtbz == true).
          aggregateMessages[Seq[(Int, Boolean)]](ctx => {
          if (ctx.srcAttr.wtbz == true)
            ctx.sendToSrc(Seq((ctx.dstAttr.old_fz, ctx.dstAttr.wtbz)))
          if (ctx.dstAttr.wtbz == true)
            ctx.sendToDst(Seq((ctx.srcAttr.old_fz, ctx.srcAttr.wtbz)))
        }, _ ++ _)
      case "new" =>
        msg = graph.subgraph(epred = ctx => ctx.srcAttr.wtbz == true || ctx.dstAttr.wtbz == true).
          aggregateMessages[Seq[(Int, Boolean)]](ctx => {
          if (ctx.srcAttr.wtbz == true)
            ctx.sendToSrc(Seq((ctx.dstAttr.new_fz, ctx.dstAttr.wtbz)))
          if (ctx.dstAttr.wtbz == true)
            ctx.sendToDst(Seq((ctx.srcAttr.new_fz, ctx.srcAttr.wtbz)))
        }, _ ++ _)
    }
    val pref = graph.vertices.join(msg).map { case (vid, (vattr, msg)) =>
      val count = msg.filter(_._1 < vattr.new_fz).size
      val mz = msg.filter { case (fz, wtbzo) =>
        wtbzo == false && fz < vattr.new_fz //&& fz!=0
      }.size
      if (count == 0)
        0
      else
        mz / count.toDouble
    }
    val prefinal = pref.reduce(_ + _) / pref.count()
    prefinal
  }

  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]): EvaluateResult[Double] = {
    TwoResult(computePREF(graph, "old"), computePREF(graph, "new"))
  }
}

/**
  * Author: weiwenda
  * Description: 异常企业的分数变化趋势，分数变低的异常企业数/所有异常企业,分数变低的异常企业数/所有分数变低企业
  * Date: 下午4:09 2017/11/28
  */
case class TENDENCY() extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {
  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]) = {
    val co1 = graph.vertices.filter { case (vid, vattr) => vattr.wtbz && vattr.new_fz < vattr.old_fz }.count()
    val co2 = graph.vertices.filter(_._2.wtbz).count()
    val co3 = graph.vertices.filter { case (vid, vattr) => vattr.old_fz > 0 && vattr.new_fz < vattr.old_fz }.count()
    TwoResult(co1 / co2.toDouble, co1 / co3.toDouble)
  }
}

/**
  * Author:weiwenda
  * Description:新评分中异常企业的总体排名情况
  * Date:22:04 2017/12/23
  */
case class RANKSCORE() extends Measurement[ResultVertexAttr, ResultEdgeAttr, EvaluateResult[Double]] {
  override def compute(graph: Graph[ResultVertexAttr, ResultEdgeAttr]) = {
    val sorted = graph.vertices.filter(_._2.old_fz > 0).sortBy(_._2.new_fz).zipWithIndex()
    val num1 = sorted.count()
    val sorted1 = sorted.filter(e => e._1._2.wtbz).map(e => e._2 / num1.toDouble)
    val num2 = sorted1.count()
    OneResult(sorted1.sum() / num2)
  }
}


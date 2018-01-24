package wwd.main

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.SparkSession

object TestDecisionTree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setJars(Seq("/Users/weiwenda/WorkSpace/IDEAWorkSpace/tpin/out/artifacts/ShannxiInfluence_jar/ShannxiInfluence.jar"))
    val session = SparkSession
      .builder
      .config(conf)
      .master("spark://MacLQ2.local:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val sc = session.sparkContext
  }
  def main1(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setJars(Seq("/Users/weiwenda/WorkSpace/IDEAWorkSpace/tpin/out/artifacts/ShannxiInfluence_jar/ShannxiInfluence.jar"))
    val session = SparkSession
      .builder
      .config(conf)
      .master("spark://MacLQ2.local:7077")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    val sc = session.sparkContext
    val input_init = sc.textFile("/Users/weiwenda/AnacondaProjects/MLBook/chapter03/lenses.txt").
      map(_.split("\\s+")).map(e => if (e.length == 5){
      e(4)="yes"
      e
    }  else{
      e(4)="no"
      e
    } )
    val age = input_init.map(e=>e(0)).distinct().collect()
    val salary = input_init.map(e=>e(1)).distinct().collect()
    val student = input_init.map(e=>e(2)).distinct().collect()
    val credit = input_init.map(e=>e(3)).distinct().collect()
    val labels = input_init.map(e=>e(4)).distinct().collect()
    val input = input_init.map { e =>
      val label = labels.indexOf(e(4)).toDouble
      val tmp = Array(
        age.indexOf(e(0)).toDouble,
        salary.indexOf(e(1)).toDouble,
        student.indexOf(e(2)).toDouble,
        credit.indexOf(e(3)).toDouble
      )
      LabeledPoint(label,Vectors.dense(tmp))
    }
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "entropy"
    val maxDepth = 5
    val maxBins = 32
    val model = DecisionTree.trainClassifier(input,2,categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    val labelAndPreds = input.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / input.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)
    // Save and load model
//    model.save(sc, "target/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/myDecisionTreeClassificationModel")
  }

}

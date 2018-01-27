package wwd.classify.impl

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, DecisionTreeClassificationModel, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import wwd.classify.ClassifierRunner
import org.apache.spark.sql.functions.max

class LogisticRegressionRunner extends ClassifierRunner {
  override def showModel(): Unit = {
    val lorModel = model.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    //annotation of david:判断特征重要性
    //    lorModel.coefficients.toArray.zipWithIndex.sortBy(_._1)(Ordering[Double].reverse).map(e=>index2Name(e._2))
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")
  }

  override def assembleFeatures(dataLabelDF: Dataset[Row]) = {
    removeFeature(Array("FDDBR_AREA", "CWFZR_AREA", "BSR_AREA", "ZCD_YB"))
    super.assembleFeatures(dataLabelDF)
  }

  override def train(trainingData: Dataset[Row]): PipelineModel = {
//    val (training: DataFrame, test: DataFrame) = loadDatasets("/user/weiwenda/data/mllib/sample_libsvm_data.txt",
//      "libsvm","", "classification", 0.2)
//    testData = test

    val sessionLocal = session
    import sessionLocal.implicits._
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")//.fit(dataLabelDF)
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val lr = new LogisticRegression()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.0)
      .setElasticNetParam(1.0)

    val startTime = System.nanoTime()
    // Fit the model
    val pipeline = new Pipeline().setStages(Array(labelIndexer,scaler,lr))
    val pipelineModel = pipeline.fit(trainingData)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")
    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
     val binarySummary = lorModel.summary.asInstanceOf[BinaryLogisticRegressionSummary]
     // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
 //    val roc = binarySummary.roc
 //    roc.show()
 //    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")
     // Set the model threshold to maximize F-Measure
     val fMeasure = binarySummary.fMeasureByThreshold
     val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
     val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
       .select("threshold").head().getDouble(0)
     lorModel.setThreshold(bestThreshold)
    pipelineModel
  }
}

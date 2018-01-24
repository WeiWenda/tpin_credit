package wwd.classify

import com.sun.org.apache.xalan.internal.utils.FeatureManager.Feature
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer

abstract class ClassifierRunner {
  @transient
  var sc: SparkContext = _
  @transient
  var session: SparkSession = _
  @transient
  var dataLabelDF: Dataset[Row] = _
  @transient
  var trainingData:Dataset[Row] = _
  @transient
  var testData:Dataset[Row] = _
  @transient
  var model:PipelineModel = _
  @transient
  val registerInfo = Array("ZCDZ_NUM","ZCD_YB",
    "SWHZDJRQ_MONTH","KYSLRQ_MONTH")
  @transient
  val scaleInfo = Array("TZF_NUM","ZCZB",
    "TZZE","CYRS",
    "FP_ZZSZYFP_XXJE_SUM","FP_ZZSZYFP_GXJE_SUM",
  "FP_ZZSZYFP_GF_SUM","FP_ZZSZYFP_XF_SUM")
  @transient
  val affiliateInfo = Array("SB_ZZS_2003_YNSE_RATIO_AVG", "SB_ZZS_2003_YDKSEHJ_RATIO_AVG",
    "SB_ZZS_2003_SJDKSE_RATIO_AVG", "SB_ZZS_2003_SF_RATIO_AVG",
    "FP_ZZSZYFP_XFJSHJ_MEAN", "FP_ZZSZYFP_XFJSHJ_MEDIAN",
    "FP_ZZSZYFP_CZGPTS_SUM", "FP_ZZSZYFP_CZKPTS_SUM")
  @transient
  val taxIndicator = Array("FDDBR_AGE","FDDBR_AREA",
    "CWFZR_AGE","CWFZR_AREA",
    "BSR_AGE","BSR_AREA",
    "GD_NUM")
  @transient
  val neighborInfo = Array("OLD_FZ","NEI_MEAN",
    "NEI_WMEAN","NEI_LNUM",
    "INDEGREE","OUTDEGREE")
  @transient
  var featuresArray = ArrayBuffer()++registerInfo++scaleInfo++affiliateInfo++taxIndicator++neighborInfo

  initialize()

  def removeFeature(feature:Array[String])={
    featuresArray --= feature
  }
  def setFeature(feature: Array[String])={
    featuresArray = ArrayBuffer()++feature
  }
  def initialize(): Unit = {
    session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sc = session.sparkContext
  }
  def assembleFeatures(dataLabelDF: Dataset[Row]): Dataset[Row] = {
    val assembler = new VectorAssembler().setInputCols(featuresArray.toArray).setOutputCol("features")
    assembler.transform(dataLabelDF)
  }

  def run(): Unit = {
    //annotation of david:加入缓存机制，如果primitiveGraph已加载则跳过
    dataLabelDF = assembleFeatures(prepareDataSet(session))
    val array = dataLabelDF.randomSplit(Array(0.7, 0.3))
    trainingData = array(0)
    testData = array(1)
    model = train(trainingData)
  }


  def evaluation(testData:Dataset[Row]) = {
    // 作出预测
    val predictions = model.transform(testData)
    // 选择（预测标签，实际标签），并计算测试误差。
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    (1.0 - accuracy)
  }

  def showModel()

  def prepareDataSet(session: SparkSession): Dataset[Row]

  def train(trainingData: Dataset[Row]):PipelineModel
}
object ClassifierRunner{
  val feature2Name = Map("ZCDZ_NUM"->"注册地纳税人个数",)
}

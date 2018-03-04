package wwd.classify


import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import wwd.utils.{HdfsTools, OracleTools, Parameters}

import scala.collection.mutable.ArrayBuffer

abstract class ClassifierRunner {
  @transient
  var sc: SparkContext = _
  @transient
  var session: SparkSession = _
  @transient
  var dataLabelDF: Dataset[Row] = _
  @transient
  var trainingData: Dataset[Row] = _
  @transient
  var testData: Dataset[Row] = _
  @transient
  var model: PipelineModel = _
  @transient
  var predictions: Dataset[Row] = _
  @transient
  var metrics: MulticlassMetrics = _

  @transient
  var featuresArray = ArrayBuffer() ++
    ClassifierRunner.registerInfo ++
    ClassifierRunner.scaleInfo ++
    ClassifierRunner.affiliateInfo ++
    ClassifierRunner.taxIndicator ++
    ClassifierRunner.neighborInfo

  initialize()

  def removeFeature(feature: Array[String]) = {
    featuresArray --= feature
  }

  def setFeature(feature: Array[String]) = {
    featuresArray = ArrayBuffer() ++ feature
  }

  def index2Name(index: Int) = {
    ClassifierRunner.feature2Name.get(featuresArray(index)).get
  }

  def initialize(): Unit = {
    session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sc = session.sparkContext
  }

  /**
    * Author: weiwenda
    * Description: 将问题企业数据重复多遍，使得分类训练集数据均衡
    * Date: 下午6:39 2018/1/24
    */
  def prepareDataSet(session: SparkSession): Dataset[Row] = {
    val path = Parameters.ModelDir + "/whole_data.parquet"
    if (!HdfsTools.Exist(sc, path)) {
      val dataFromOracle = session.read.format("jdbc").
        options(OracleTools.options + (("dbtable", "WWD_DECISION_TREE"))).load().
        drop("NSRDZDAH").withColumnRenamed("WTBZ", "label")
      import session.implicits._
      val wrong = dataFromOracle.filter($"label" === "Y")
      val good = dataFromOracle.filter($"label" === "N")
      val duplicateTime = good.count() / wrong.count()
      var duplicated = wrong
      for (i <- Range(0, duplicateTime.toInt))
        duplicated = duplicated.union(wrong)
      val toSave = good.union(duplicated)
      toSave.write.format("parquet").save(path)
    }
    session.read.load(path)
  }

  def assembleFeatures(dataLabelDF: Dataset[Row]): Dataset[Row] = {
    val assembler = new VectorAssembler()
      .setInputCols(featuresArray.toArray)
      .setOutputCol("features")
    assembler.transform(dataLabelDF)
  }

  /**
    * Author: weiwenda
    * Description: prepareDataSet读入数据并使数据均衡化
    * assembleFeatures用来生成向量
    * train对属性进行String index化，离散值 index化、决策树拟合、index 标签化
    * Date: 下午5:49 2018/1/25
    */
  def run(): Unit = {
    //annotation of david:加入缓存机制，如果primitiveGraph已加载则跳过
    dataLabelDF = assembleFeatures(prepareDataSet(session))
    val array = dataLabelDF.randomSplit(Array(0.7, 0.3))
    trainingData = array(0)
    testData = array(1)
    model = train(trainingData)
    // 作出预测
    predictions = model.transform(testData).cache()
  }

  /**
    * Author: weiwenda
    * Description: f1|weightedPrecision|weightedRecall|accuracy|maxtrix
    * Date: 下午9:41 2018/1/25
    */
  def evaluate(metricName: String = "accuracy"): String = {
    // 选择（预测标签，实际标签），并计算测试误差。
    if (metrics == null) {
      val predictionAndLabels =
        predictions.select(col("prediction"), col("indexedLabel").cast(DoubleType)).rdd.map {
          case Row(prediction: Double, label: Double) => (prediction, label)
        }
      metrics = new MulticlassMetrics(predictionAndLabels)
    }
    val metric = metricName match {
      case "f1" => metrics.weightedFMeasure
      case "weightedPrecision" => metrics.weightedPrecision
      case "weightedRecall" => metrics.weightedRecall
      case "accuracy" => metrics.accuracy
      case "maxtrix" => metrics.confusionMatrix
    }
    metric.toString
  }

  def saveModel(overwrite: Boolean = true): Unit = {
    val path = Parameters.ModelDir
    if (overwrite)
      model.write.overwrite().save(s"${path}/${this.getClass.getSimpleName}")
    else
      model.save(s"${path}/${this.getClass.getSimpleName}")
  }

  def loadAndRerun(): Unit = {
    val path = Parameters.ModelDir
    dataLabelDF = assembleFeatures(prepareDataSet(session))
    val array = dataLabelDF.randomSplit(Array(0.7, 0.3))
    trainingData = array(0)
    testData = array(1)
    model = PipelineModel.load(s"${path}/${this.getClass.getSimpleName}")
    // 作出预测
    predictions = model.transform(testData)
  }

  def showModel()

  def train(trainingData: Dataset[Row]): PipelineModel
}

object ClassifierRunner {
  /**
  * Author: weiwenda
  * Description: 此处对离散的理解有误
  * Date: 下午8:08 2018/1/30
  */
  def ChiSquareTest(df: DataFrame, featuresCol: String = "Cfeatures", labelCol: String = "indexedLabel"): Unit = {
    val assembler = new VectorAssembler()
      .setInputCols(categoryInfo)
      .setOutputCol("Cfeatures")
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val data=labelIndexer.transform(assembler.transform(df))
    val chi = org.apache.spark.ml.stat.ChiSquareTest.test(data, featuresCol, labelCol).head
    println("pValues = " + chi.getAs[Vector](0))
    println("degreesOfFreedom = " + chi.getSeq[Int](1).mkString("[", ",", "]"))
    println("statistics = " + chi.getAs[Vector](2))
  }

  def CorrelationTest(df: DataFrame): Unit = {
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println("Pearson correlation matrix:\n" + coeff1.toString)
    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println("Spearman correlation matrix:\n" + coeff2.toString)
  }

  val feature2Name = Map("ZCDZ_NUM" -> "注册地纳税人个数", "ZCD_YB" -> "注册地址", "SWHZDJRQ_MONTH" -> "登记至今月数", "KYSLRQ_MONTH" -> "开业至今月数",
    "TZF_NUM" -> "投资方数量", "ZCZB" -> "注册资本", "TZZE" -> "投资总额", "CYRS" -> "从业人数",
    "FP_ZZSZYFP_XXJE_SUM" -> "销售总额", "FP_ZZSZYFP_GXJE_SUM" -> "购买总额", "FP_ZZSZYFP_GF_SUM" -> "销方数量", "FP_ZZSZYFP_XF_SUM" -> "购方数量",
    "SB_ZZS_2003_YNSE_RATIO_AVG" -> "应纳税额变动率", "SB_ZZS_2003_YDKSEHJ_RATIO_AVG" -> "应抵扣税额变动率", "SB_ZZS_2003_SJDKSE_RATIO_AVG" -> "实际抵扣税额变动率", "SB_ZZS_2003_SF_RATIO_AVG" -> "税负变动率",
    "FP_ZZSZYFP_XFJSHJ_MEAN" -> "月度平均价税合计", "FP_ZZSZYFP_XFJSHJ_MEDIAN" -> "月度居中价税合计", "FP_ZZSZYFP_CZGPTS_SUM" -> "购项发票数量", "FP_ZZSZYFP_CZKPTS_SUM" -> "销项发票数量",
    "FDDBR_AGE" -> "法人年龄", "FDDBR_AREA" -> "法人省份", "CWFZR_AGE" -> "财务负责人年龄", "CWFZR_AREA" -> "财务负责人省份",
    "BSR_AGE" -> "办税人年龄", "BSR_AREA" -> "办税人省份", "GD_NUM" -> "股东数量", "OLD_FZ" -> "纳税信用评分",
    "NEI_MEAN" -> "邻居平均评分", "NEI_WMEAN" -> "邻居加权评分", "NEI_LNUM" -> "低分邻居数量", "INDEGREE" -> "影响网络入度",
    "OUTDEGREE" -> "影响网络出度")
  val categoryInfo = Array("FDDBR_AREA", "CWFZR_AREA", "BSR_AREA", "ZCD_YB")
  val registerInfo = Array("ZCDZ_NUM", "ZCD_YB",
    "SWHZDJRQ_MONTH", "KYSLRQ_MONTH")
  val scaleInfo = Array("TZF_NUM", "ZCZB",
    "TZZE", "CYRS",
    "FP_ZZSZYFP_XXJE_SUM", "FP_ZZSZYFP_GXJE_SUM",
    "FP_ZZSZYFP_GF_SUM", "FP_ZZSZYFP_XF_SUM")
  val affiliateInfo = Array("SB_ZZS_2003_YNSE_RATIO_AVG", "SB_ZZS_2003_YDKSEHJ_RATIO_AVG",
    "SB_ZZS_2003_SJDKSE_RATIO_AVG", "SB_ZZS_2003_SF_RATIO_AVG",
    "FP_ZZSZYFP_XFJSHJ_MEAN", "FP_ZZSZYFP_XFJSHJ_MEDIAN",
    "FP_ZZSZYFP_CZGPTS_SUM", "FP_ZZSZYFP_CZKPTS_SUM")
  val taxIndicator = Array("FDDBR_AGE", "FDDBR_AREA",
    "CWFZR_AGE", "CWFZR_AREA",
    "BSR_AGE", "BSR_AREA",
    "GD_NUM")
  val neighborInfo = Array("OLD_FZ", "NEI_MEAN",
    "NEI_WMEAN", "NEI_LNUM",
    "INDEGREE", "OUTDEGREE")
}

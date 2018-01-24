package wwd.classify.impl

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import wwd.classify.ClassifierRunner
import wwd.utils.OracleTools
/**
  * Author: weiwenda
  * Description: 使用决策树进行最后的分类,用ml类库
  * Date: 下午9:23 2018/1/17
  */
class DecsionTreeClassifier extends ClassifierRunner{
  /**
    * Author: weiwenda
    * Description: 将问题企业数据重复多遍，使得分类训练集数据均衡
    * Date: 下午6:39 2018/1/24
    */
  override def prepareDataSet(session: SparkSession): Dataset[Row] = {
    val dataFromOracle = session.read.format("jdbc").
      options(OracleTools.options + (("dbtable", "WWD_DECISION_TREE"))).load().
      drop("NSRDZDAH").withColumnRenamed("WTBZ","label")
    import session.implicits._
    val wrong = dataFromOracle.filter($"label"==="Y")
    val good = dataFromOracle.filter($"label"==="N")
    val duplicateTime = good.count()/wrong.count()
    var duplicated = wrong
    for(i <- Range(0,duplicateTime.toInt))
      duplicated = duplicated.union(wrong)
    good.union(duplicated)
  }
  override def train(trainingData: Dataset[Row]):PipelineModel= {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataLabelDF)
    // 自动识别分类的特征，并对它们进行索引
    // 具有大于8个不同的值的特征被视为连续。
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(8).fit(dataLabelDF)
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("entropy") // 不纯度
      .setMaxBins(100) // 离散化"连续特征"的最大划分数
      .setMaxDepth(5) // 树的最大深度
      .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(10) //每个节点包含的最小样本数
      .setSeed(123456)
    // 将索引标签转换回原始标签
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    pipeline.fit(trainingData)
  }
  override def showModel()={
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  }

}

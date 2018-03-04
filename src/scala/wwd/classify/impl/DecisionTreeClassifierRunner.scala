package wwd.classify.impl

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorIndexer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import wwd.classify.ClassifierRunner
import wwd.utils.OracleTools
import org.apache.spark.ml.tree._
import spray.json._
import wwd.classify.impl.DecisionTreeClassifierRunner.TreeNode
/**
  * Author: weiwenda
  * Description: 使用决策树进行最后的分类,用ml类库
  * Date: 下午9:23 2018/1/17
  */
class DecisionTreeClassifierRunner extends ClassifierRunner{

  override def train(trainingData: Dataset[Row]):PipelineModel= {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataLabelDF)
    // 自动识别分类的特征，并对它们进行索引
    // 具有大于8个不同的值的特征被视为连续。
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(56).fit(dataLabelDF)
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("entropy") // 不纯度
      .setMaxBins(56) // 离散化"连续特征"的最大划分数
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
  /**
  * Author: weiwenda
  * Description: 将DecisionTreeClassificationModel转化为TreeNode，再转化为JS AST，再转化为Json String,最终用于D3可视化
  * Date: 下午4:24 2018/1/25
  */
  def toJson()={
    import wwd.classify.impl.DecisionTreeClassifierRunner.MyJsonProtocol._
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    DecisionTreeClassifierRunner.index2Name = index2Name
    val ast = DecisionTreeClassifierRunner.deepCopy(treeModel.rootNode,"Root").toJson.prettyPrint
    println(ast)
  }
}
object DecisionTreeClassifierRunner{
  /**
  * Author: weiwenda
  * Description: 伴生对象中的属性均为toJson函数服务
  * Date: 下午5:49 2018/1/25
  */
  var index2Name:Int=>String = null
  case class TreeNode(name:String,children:Option[List[TreeNode]])
  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val fooFormat: JsonFormat[TreeNode] = lazyFormat(jsonFormat(TreeNode, "name", "children"))
  }
  def deepCopy(node:Node,nodeName:String): TreeNode ={
    if(node.isInstanceOf[InternalNode]){
      val tmp = node.asInstanceOf[InternalNode]
      val left = deepCopy(tmp.leftChild,splitToString(tmp.split, left = true))
      val right =  deepCopy(tmp.rightChild,splitToString(tmp.split, left = false))
      TreeNode(nodeName,Option(List(left,right)))
    }else{
      TreeNode(nodeName,Option(List(TreeNode(node.toString,Option.empty))))
    }
  }
  private def splitToString(split: Split, left: Boolean): String = {
    val featureStr = s"${index2Name(split.featureIndex)}"
    split match {
      case contSplit: ContinuousSplit =>
        if (left) {
          s"$featureStr <= ${contSplit.threshold}"
        } else {
          s"$featureStr > ${contSplit.threshold}"
        }
      case catSplit: CategoricalSplit =>
        val categoriesStr = catSplit.leftCategories.mkString("{", ",", "}")
        if (left) {
          s"$featureStr in $categoriesStr"
        } else {
          s"$featureStr not in $categoriesStr"
        }
    }
  }

}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint

object Main {
  val CATEGORIES = 20

  def my_compare(a: Double, b: Double) : Double = {
    if (a == b) 1.0
    else 0.0
  }

  def train(): Unit = {
    val conf = new SparkConf().setAppName("SparkSVM_train").setMaster("local")
    val sc = new SparkContext(conf)

    val trainRDD = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/train.txt")

    for (i <- 1 to CATEGORIES) {
      val label = i.asInstanceOf[Double]
      val binaryTrainRDD = trainRDD.map(s => s.copy(my_compare(label, s.label), s.features))
      val numIterations = 100
      val model = SVMWithSGD.train(binaryTrainRDD, numIterations)

      model.save(sc, "hdfs://localhost:9000/SVM_models/" + i.toString + ".model")
    }
  }

  def test(): Unit = {
    val conf = new SparkConf().setAppName("SparkSVM_test").setMaster("local")
    val sc = new SparkContext(conf)

    val trainRDD = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/test.txt")

    var model_list:Array[SVMModel] = new Array[SVMModel](20)

    for (i <- 1 to CATEGORIES) {
      val model = SVMModel.load(sc, "hdfs://localhost:9000/SVM_models/" + i.toString + ".model")
      model.clearThreshold()
      model_list(i-1) = model
    }

    val predict_result = trainRDD.map(point => {
      var result = 0.0
      var value = -1.0
      for (i <- 1 to CATEGORIES) {
        val re = model_list(i-1).predict(point.features)
        if (re > value) {
          result = i.asInstanceOf[Double]
          value = re
        }
      }
      result.asInstanceOf[Int]
    })

    predict_result.saveAsTextFile("hdfs://localhost:9000/svm_result")
  }

  def main(args: Array[String]): Unit = {
    //train()
    test()
  }
}

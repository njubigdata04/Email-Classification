import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSVM").setMaster("local")
    val sc = new SparkContext(conf)

    val trainRDD = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/cache.txt")

    println(trainRDD.toString())

    val label = 9

    //trainRDD.foreach( point => if(point.label == label) point.label = 1)



    //val testRDD = MLUtils.loadLibSVMFile(sc, "hdfs://localhost:9000/data/test_data.txt")



    //TODO: train N SVMs(OVR), and predict by voting.



    val numIterations = 100
    //val model = SVMWithSGD.train(trainRDD, numIterations)
/*
    val predictionAndLabel = testRDD.map { point =>
      val score = model.predict(point.features)
      (score, point.label, point.features)
    }

    val showPredict = predictionAndLabel.take(50)
    println("Prediction" + "\t" + "Label" + "\t" + "Data")
    for (i <- 0 to showPredict.length - 1) {
      println(showPredict(i)._1 + "\t" + showPredict(i)._2 + "\t" + showPredict(i)._3)
    }

    // 误差计算
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testRDD.count()
    println("Accuracy = " + accuracy)*/
  }
}

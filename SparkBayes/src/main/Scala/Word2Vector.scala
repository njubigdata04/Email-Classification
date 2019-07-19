import NaiveBayes.RawDataRecord
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

object Word2Vector {
  def main(args: Array[String]) {

    val inputpath = "file:///D:/课程学习/大三下/大数据实验/task3/dataset/purefiles"
    //val inputpath =  "file:///D:/课程学习/大三下/大数据实验/task3/simpletest"
    //val inputpath = "file:///D:/课程学习/大三下/大数据实验/task3/dataset/simple"
    val testpath = "file:///D:/课程学习/大三下/大数据实验/task3/dataset/testpurefiles/purefiles"
    //val testpath = "file:///D:/课程学习/大三下/大数据实验/task3/dataset/simple"

    val sparkSession = SparkSession.builder().appName("NaiveBayes").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔.flatMap(line=>line.split(" "))
    val fileTu = sc.wholeTextFiles(inputpath).map(file =>(file._1.split("-").last,file._2.split("\n")))
    //fileTu.take(3).foreach(println)
    var training = fileTu
    import sparkSession.implicits._
    var trainset = training.toDF("category","text")

    //准备测试集
    val testfileTu = sc.wholeTextFiles(testpath).map(file =>(file._1.split("-").last,file._2.split("\n")))
    //fileTu.take(3).foreach(println)
    import sparkSession.implicits._
    var testset = testfileTu.toDF("category","text")

    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("features").
      setVectorSize(100).
      setMinCount(0)
    val w2vmodel = word2Vec.fit(trainset)
    //转换
    var trainData = w2vmodel.transform(trainset)


    trainData.select($"category", $"text", $"features").show(5,false)
    var trainDataRdd = trainData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }
    trainDataRdd.write.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/Word2Vec-train-df100")
    //trainDataRdd.show(false)
    //测试集转换
    var testData = w2vmodel.transform(testset)
    //testData.select($"category", $"words", $"features").show(false)
    var testDataRdd = testData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }
    testDataRdd.write.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/Word2Vec-test-df100")
    //testDataRdd.show(false)


    val model = new NaiveBayes().setFeaturesCol("features").setModelType("multinomial").fit(trainDataRdd)
    model.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/Word2VectModel100")
    /*val model = NaiveBayesModel.load("file:///D:/课程学习/大三下/大数据实验/task3/dataset/multinomialmodelhash50000")*/
    val testpredictionAndLabel = model.transform(testDataRdd)
    testpredictionAndLabel.show(false)
    val rddresult = testpredictionAndLabel.select($"prediction", $"label").rdd
    val re = rddresult.map{case (line)=>(line(0).toString.toDouble, line(1).toString.toDouble)}
    println("RDD")
    re.take(4).foreach(println)
    val metric = new MulticlassMetrics(re)
    println("test accuracy" + metric.accuracy)
    for (i <- 0 to 19) println("test label " + i + " recall" + metric.recall(i))
    for (i <- 0 to 19) println("test label" + i + " precision" + metric.precision(i))


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(testpredictionAndLabel)

    println("Test set accuracy = " + accuracy)
  }
}

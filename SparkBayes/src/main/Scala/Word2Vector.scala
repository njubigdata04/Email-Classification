import NaiveBayes.RawDataRecord
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayesModel
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
    val fileTu = sc.wholeTextFiles(inputpath).map(file =>(file._1.split("-").last,file._2))
    //fileTu.take(3).foreach(println)
    var training = fileTu.map {
      line =>
        var filename = line._1
        var second = line._2
        RawDataRecord(filename,second)
    }
    import sparkSession.implicits._
    var trainset = training.toDF("category","text")

    //准备测试集
    val testfileTu = sc.wholeTextFiles(testpath).map(file =>(file._1.split("-").last,file._2))
    //fileTu.take(3).foreach(println)
    var testing = testfileTu.map {
      line =>
        var filename = line._1
        var second = line._2
        RawDataRecord(filename,second)
    }
    import sparkSession.implicits._
    var testset = testing.toDF("category","text")

    //分好的词转成数组
    //var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //val re = tokenizer.transform(trainset)
    //re.show(false)
    val regexTokenizer = new RegexTokenizer().
      setInputCol("text").setOutputCol("words")
      .setPattern("\\s") // alternatively .setPattern("\\w+").setGaps(false)
    //val ree = regexTokenizer.transform(trainset)
    //ree.show(false)

    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)
    //val hashre = hashingTF.transform(re)
    //hashre.show(false)
    //val hashree = hashingTF.transform(ree)
    //hashree.show(false)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val pipeline = new Pipeline()setStages(Array(regexTokenizer,hashingTF,idf))
    var idfModel = pipeline.fit(trainset)

    //转换
    var trainData =idfModel.transform(trainset)


    trainData.select($"category", $"words", $"features").show(5,false)
    var trainDataRdd = trainData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }
    trainDataRdd.write.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/regex-train-df10000")
    //测试集转换
    var testData = idfModel.transform(testset)
    //testData.select($"category", $"words", $"features").show(false)
    var testDataRdd = testData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }
    testDataRdd.write.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/regex-test-df10000")
    //testDataRdd.show(false)


    //val model = new NaiveBayes().setFeaturesCol("features").setModelType("multinomial").fit(trainDataRdd)
    //model.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/multinomialmodelhash100000")
    /*val model = NaiveBayesModel.load("file:///D:/课程学习/大三下/大数据实验/task3/dataset/multinomialmodelhash50000")
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
  */
  }
}

import NaiveBayes.RawDataRecord
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

object Example {
  def main(args: Array[String]) {

    val inputpath = "file:///D:/课程学习/大三下/大数据实验/task3/dataset/purefiles"
    //val inputFile =  "file:///D:/课程学习/大三下/大数据实验/task3/file1.txt"
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
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(5000)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    val pipeline = new Pipeline()setStages(Array(tokenizer,hashingTF,idf))
    var idfModel = pipeline.fit(trainset)

    //转换
    var trainData =idfModel.transform(trainset)


    trainData.select($"category", $"words", $"features").show(5,false)
    var trainDataRdd = trainData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }

    //测试集转换
    var testData = idfModel.transform(testset)
    //testData.select($"category", $"words", $"features").show(false)
    var testDataRdd = testData.select($"category", $"features").map{
      case Row(label:String, features:Vector)=>
        LabeledPoint(label.toDouble - 1, Vectors.dense(features.toArray))
    }

    //testDataRdd.show(false)


    //val model = new NaiveBayes().setFeaturesCol("features").setModelType("multinomial").fit(trainDataRdd)
    //model.save("file:///D:/课程学习/大三下/大数据实验/task3/dataset/multinomialmodelhash100000")
    val model = NaiveBayesModel.load("file:///D:/课程学习/大三下/大数据实验/task3/dataset/multinomialmodelhash50000")
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

    //val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    //val sc = new SparkContext(conf)
    //val textFile = sc.textFile(inputFile)
    //val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    //val training = sc.wholeTextFiles(inputpath)
    //var trainset = training.map{case (filename:String, text:String)=>RawDataRecord(filename.split("/"), text)}.toDF
    //打印文件的数量
    //val text = trainset.map { case (file, text) => (file, text.split("\n")) }
    //val tokens = rdd.map((filename :String, doc : String) => (filename, tokenize(doc)))
    //rdd.take(5).foreach(println)
    //text.take(5).foreach { case (file, text: Array[String]) => println(text.last) }
  }
}

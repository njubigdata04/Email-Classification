import org.apache.spark.{SparkConf, SparkContext}

object Main extends App{
  override def main(args: Array[String]): Unit = {
    val logFile = "hdfs://localhost:9000/data/exp3_sample_data/4.txt"
    val conf = new SparkConf().setAppName("WordCount_Spark").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(logFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(ascending = false).map(x => (x._2,x._1))
    wordcount.saveAsTextFile("hdfs://localhost:9000/data/spark_wordcount.txt")
    sc.stop()
  }
}

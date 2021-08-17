import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object test{
  def main(args:Array[String]): Unit ={
    val fileCount = 3
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var resultRDD:RDD[(String,Int)] = null
    for(index <- 0 until fileCount) {
      val file = sc.textFile("file:///d:/" + index + ".txt")
      val tempRDD = file.flatMap(line => line.split(" "))
        .map(word => (word, (index, 1)))
        .map(word => (word._1 + " " + word._2._1, word._2._2))
        .reduceByKey((a, b) => a + b)
      if (resultRDD != null) {
        resultRDD = sc.union(resultRDD, tempRDD)
      }
      else {
        resultRDD = tempRDD
      }
    }
    val result = resultRDD
      .map(wordInfo => (wordInfo._1.split(" ")(0),(wordInfo._1.split(" ")(1).toInt,wordInfo._2)))
      .groupByKey().collect()
  }
}
package springnz.sparkplug.examples

import org.apache.spark.{ SparkContext, SparkConf }
import springnz.sparkplug.util.Logging

object SimpleLetterCount extends Logging {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleApplication")
      .setMaster("spark://akldsk001.local:7077")
    val sc = new SparkContext(conf)

    val sentence = "There is nothing either good or bad, but thinking makes it so".split(" ")
    val rdd = sc.parallelize(sentence)
    val numAs = rdd.filter(line ⇒ line.contains("a")).count()
    val numBs = rdd.filter(line ⇒ line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

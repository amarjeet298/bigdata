package t10.iss.nus

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Test IO to wasb
  */
object App {

  def main(args: Array[String]) {

    // Load properties
    val config = ConfigFactory.load().getConfig("app.env")

    val fileName = config.getString("fileLocation")
    println(s"My secret value is $fileName")

    val host = config.getString("sparkHost")

    val conf = new SparkConf().setAppName("WordCount Application").setMaster(host)
    val sc = new SparkContext(conf);



    //val fileName = args(value);
    val lines = sc.textFile(fileName).cache()
    val c = lines.count()

    println(s"There are $c lines in $fileName")

  }
}

  /* def main (arg: Array[String]): Unit = {
   val conf = new SparkConf().setAppName("WASBIOTest")
   val sc = new SparkContext(conf)

   val rdd = sc.textFile("wasb:///HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv")

   //find the rows which have only one digit in the 7th column in the CSV
   val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

   rdd1.saveAsTextFile("wasb:///HVACout")
 }*/

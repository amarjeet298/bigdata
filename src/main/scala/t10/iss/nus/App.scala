package t10.iss.nus

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Test IO to wasb
  */
object WasbIOTest {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount Application").setMaster("local")
    val sc = new SparkContext(conf);

    val fileName = args(0);
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

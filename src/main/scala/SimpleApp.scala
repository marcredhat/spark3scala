/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession



object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //val spark2 = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.sqlContext.implicits._
    val UDAF = spark.range(100).withColumn("group", $"id"%2).groupBy("group").agg(MeanUdaf($"id").as("mean")).show
    
    val b1 = s""" $UDAF/* """
    println(b1)
    spark.stop()
  }
}




/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._

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
    


    val meanAggregator = new Aggregator[Long, AggregatorState, Double]() {
    // Initialize your buffer
    def zero: AggregatorState = AggregatorState(0L, 0L)

    // This is how to update your buffer given an input
    def reduce(b: AggregatorState, a: Long): AggregatorState = AggregatorState(b.sum + a, b.count + 1)

    // This is how to merge two buffers
    def merge(b1: AggregatorState, b2: AggregatorState): AggregatorState = AggregatorState(b1.sum + b2.sum, b1.count + b2.count)

    // This is where you output the final value, given the final value of your buffer
    def finish(reduction: AggregatorState): Double = reduction.sum / reduction.count

    // Used to encode your buffer
    def bufferEncoder: Encoder[AggregatorState] = implicitly(ExpressionEncoder[AggregatorState])

    // Used to encode your output
    def outputEncoder: Encoder[Double] = implicitly(ExpressionEncoder[Double])
}
    val meanUdaf: UserDefinedFunction = udaf(meanAggregator)
    val mean = spark.range(100).withColumn("group", col("id")%2).groupBy("group").agg(meanUdaf(col("id")).as("mean")).show 
    val b2 = s""" $mean/* """
    println(b2)

    spark.stop()
}  
}




/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._


//https://medium.com/teads-engineering/updating-to-spark-3-0-in-production-f0b98aa2014d

object SimpleApp {
def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    import spark.sqlContext.implicits._



    //A UserDefinedAggregateFunction, commonly called UDAF, is particularly useful to define custom aggregations, 
    //such as averaging sparse arrays. See https://medium.com/teads-engineering/apache-spark-udaf-could-be-an-option-c2bc25298276

    //In Spark 3, the sql.expressions.UserDefinedAggregateFunction API has been deprecated in favor of the Aggregator API 
    //(based on algebird Aggregator).

    println("Using deprecated UserDefinedAggregateFunction API:")
    val UDAF = spark.range(100).withColumn("group", $"id"%2).groupBy("group").agg(MeanUdaf($"id").as("mean")).show
    

    
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
    //udaf - check definition
    val meanUdaf: UserDefinedFunction = udaf(meanAggregator)

    println("Using the Aggregator API:")

    val mean = spark.range(100).withColumn("group", col("id")%2).groupBy("group").agg(meanUdaf(col("id")).as("mean")).show 
    //val b2 = s""" $mean/* """
    //println(b2)

    spark.stop()
}  
}




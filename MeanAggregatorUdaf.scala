import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._

case class AggregatorState(sum: Long, count: Long) {

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


/**
+-----+----+
|group|mean|
+-----+----+
|    0|49.0|
|    1|50.0|
+-----+----+
*/

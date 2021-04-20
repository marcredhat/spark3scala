import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object MeanUdaf extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("id", LongType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  def bufferSchema: StructType = StructType(
    StructField("sum", LongType) ::
    StructField("count", LongType) :: Nil
  )

  // This is the output type of your aggregatation function.
  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // This is how to update your buffer schema given an input.
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  // This is how to merge two objects with the bufferSchema type.
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  def evaluate(buffer: Row): Any =
    (buffer.getLong(0) / buffer.getLong(1)).toDouble
}

//spark.range(100).withColumn("group", $"id"%2).groupBy("group").agg(MeanUdaf($"id").as("mean")).show
/**
+-----+----+
|group|mean|
+-----+----+
|    0|49.0|
|    1|50.0|
+-----+----+
*/

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions._



/**
+-----+----+
|group|mean|
+-----+----+
|    0|49.0|
|    1|50.0|
+-----+----+
*/

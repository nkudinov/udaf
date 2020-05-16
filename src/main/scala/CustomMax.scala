import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object CustomMax extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(StructField("a", LongType, true)))

  override def bufferSchema: StructType = StructType(Seq(StructField("b", LongType, true)))

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, Long.MinValue)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =  if (!input.isNullAt(0))
    buffer.update(0, Math.max(input.getLong(0),buffer.getLong(0)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =  {
    buffer1.update(0, Math.max( buffer1.getLong(0), buffer2.getLong(0)))
  }

  override def evaluate(buffer: Row): Any = buffer.getLong(0)
}
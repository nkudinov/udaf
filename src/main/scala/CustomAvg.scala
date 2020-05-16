import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object CustomAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(StructField("", DoubleType, true)))

  override def bufferSchema: StructType = StructType(Seq(StructField("cnt", LongType, true),StructField("acc", DoubleType, true)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getLong(0)+1)
    buffer.update(1,buffer.getDouble(1) + input.getDouble(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer2.getLong(0))
    buffer1.update(1, buffer2.getDouble(1))
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(1)/buffer.getLong(0)
}

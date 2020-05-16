import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
object Main extends App {
  val spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .config("spark.sql.codegen.wholeStage", "false")
    // .config("spark.sql.codegen.useIdInClassName","true")
    .config("spark.sql.shuffle.partitions", "1")
    //.config("spark.memory.offHeap.enabled","true")
    //.config("spark.memory.offHeap.size","10000000000")
    .appName("MainApplication Demo").master("local[3]").getOrCreate
  import spark.implicits._

  val ds0 = spark.sqlContext.createDataset(spark.sparkContext.parallelize(Seq(1,2,3,2,2,2,2,1,1,1,11,10),2))

  val ds2 = ds0.groupBy($"value" % 2).agg(CustomMax2("value").toColumn)
  ds2.show()
}

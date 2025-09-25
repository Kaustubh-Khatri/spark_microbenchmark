import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("ARM Benchmark")
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", "200")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .getOrCreate()

def benchmark(name: String)(f: => Unit): Unit = {
  val start = System.nanoTime()
  f
  val end = System.nanoTime()
  println(s"$name took ${(end - start) / 1e9} seconds")
}

println("Starting ARM Performance Benchmark Suite")
println(s"Spark version: ${spark.version}")
println(s"Java version: ${System.getProperty("java.version")}")
println(s"Available cores: ${Runtime.getRuntime.availableProcessors}")

// (a) Transformation Benchmark
println("\n" + "=" * 80)
println("(a) Transformation Benchmark")
println("=" * 80)

benchmark("Scala Transformation") {
  val data = spark.range(1, 100000000)
  val result = data.filter(col("id") % 2 === 0).count()
  println(s"Count: $result")
}

// (b) SQL Query Benchmark
println("\n" + "=" * 80)
println("(b) SQL Query Benchmark")
println("=" * 80)

benchmark("Scala SQL Query") {
  val df = spark.range(1, 50000000).withColumn("val", col("id") * 2)
  df.createOrReplaceTempView("t")
  spark.sql("SELECT avg(val) FROM t").show()
}

// (c) Parquet I/O Benchmark
println("\n" + "=" * 80)
println("(c) Parquet I/O Benchmark")
println("=" * 80)

benchmark("Scala Parquet I/O") {
  val df = spark.range(1, 10000000).withColumn("val", col("id") * 3)
  df.write.mode("overwrite").parquet("test_parquet_scala")
  val readDf = spark.read.parquet("test_parquet_scala")
  println(s"Parquet Count: ${readDf.count()}")
}

println("\n" + "=" * 80)
println("Benchmark Suite Completed Successfully")
println("=" * 80)

spark.stop()
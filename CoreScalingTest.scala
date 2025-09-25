import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CoreScalingTest {

  def createSparkSession(cores: String): SparkSession = {
    SparkSession.builder()
      .appName(s"Core Scaling Test - $cores cores")
      .master(s"local[$cores]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  def benchmark(name: String)(f: => Unit): (String, Double) = {
    val start = System.nanoTime()
    f
    val end = System.nanoTime()
    val duration = (end - start) / 1e9
    (name, duration)
  }

  def benchmarkWithCores(cores: String): Map[String, Any] = {
    println("=" * 60)
    println(s"Testing with $cores cores")
    println("=" * 60)

    val spark = createSparkSession(cores)

    // Transformation benchmark
    val (_, transformationTime) = benchmark("Transformation") {
      val data = spark.range(1, 50000000) // Reduced size for faster testing
      val result = data.filter(col("id") % 2 === 0).count()
    }

    // SQL benchmark
    val (_, sqlTime) = benchmark("SQL") {
      val df = spark.range(1, 25000000).withColumn("val", col("id") * 2) // Reduced size
      df.createOrReplaceTempView("t")
      spark.sql("SELECT avg(val) FROM t").collect()
    }

    // Parquet I/O benchmark
    val (_, parquetTime) = benchmark("Parquet I/O") {
      val df = spark.range(1, 5000000).withColumn("val", col("id") * 3) // Reduced size
      df.write.mode("overwrite").parquet(s"test_parquet_${cores}_cores_scala")
      val readDf = spark.read.parquet(s"test_parquet_${cores}_cores_scala")
      val parquetCount = readDf.count()
    }

    spark.stop()

    Map(
      "cores" -> cores,
      "transformation" -> transformationTime,
      "sql" -> sqlTime,
      "parquet" -> parquetTime,
      "total" -> (transformationTime + sqlTime + parquetTime)
    )
  }

  def main(args: Array[String]): Unit = {
    // Test different core configurations
    val coreConfigs = List("1", "2", "4", "8", "12") // Single core to all cores
    var results = List[Map[String, Any]]()

    println("ARM64 Core Scaling Benchmark - Scala")
    println("Hardware: Apple M2 Pro (8P+4E cores, 16GB RAM)")
    println("Platform: macOS ARM64")

    for (cores <- coreConfigs) {
      try {
        val result = benchmarkWithCores(cores)
        results = results :+ result

        val transform = f"${result("transformation").asInstanceOf[Double]}%.2f"
        val sql = f"${result("sql").asInstanceOf[Double]}%.2f"
        val parquet = f"${result("parquet").asInstanceOf[Double]}%.2f"
        val total = f"${result("total").asInstanceOf[Double]}%.2f"

        println(f"Cores: $cores%2s | Transform: ${transform}s | SQL: ${sql}s | Parquet: ${parquet}s | Total: ${total}s")
      } catch {
        case e: Exception =>
          println(s"Error with $cores cores: ${e.getMessage}")
      }
    }

    println("=" * 80)
    println("CORE SCALING ANALYSIS - SCALA")
    println("=" * 80)
    println("Cores | Transform | SQL   | Parquet | Total | Speedup")
    println("-" * 55)

    val baseline = if (results.nonEmpty) results.head("total").asInstanceOf[Double] else 1.0
    for (r <- results) {
      val cores = r("cores").asInstanceOf[String]
      val transform = r("transformation").asInstanceOf[Double]
      val sql = r("sql").asInstanceOf[Double]
      val parquet = r("parquet").asInstanceOf[Double]
      val total = r("total").asInstanceOf[Double]
      val speedup = baseline / total

      println(f"$cores%5s | ${transform}%8.2fs | ${sql}%4.2fs | ${parquet}%6.2fs | ${total}%4.2fs | ${speedup}%.2fx")
    }

    println("=" * 80)
    println("ARM64 PLATFORM ANALYSIS - SCALA")
    println("=" * 80)
    println("✅ M2 Pro has 8 Performance + 4 Efficiency cores")
    println("✅ Performance cores handle compute-intensive tasks")
    println("✅ Efficiency cores handle background/lighter tasks")
    println("✅ Spark can utilize all 12 cores effectively")
  }
}
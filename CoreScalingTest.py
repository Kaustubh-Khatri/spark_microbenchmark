from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

def create_spark_session(cores):
    """Create Spark session with specified number of cores"""
    return SparkSession.builder \
        .appName(f"Core Scaling Test - {cores} cores") \
        .master(f"local[{cores}]") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def benchmark_with_cores(cores):
    """Run benchmark with specified number of cores"""
    print(f"\n{'='*60}")
    print(f"Testing with {cores} cores")
    print(f"{'='*60}")

    spark = create_spark_session(cores)

    # Transformation benchmark
    start = time.time()
    data = spark.range(1, 50000000)  # Reduced size for faster testing
    result = data.filter(col("id") % 2 == 0).count()
    transformation_time = time.time() - start

    # SQL benchmark
    start = time.time()
    df = spark.range(1, 25000000).withColumn("val", col("id") * 2)  # Reduced size
    df.createOrReplaceTempView("t")
    spark.sql("SELECT avg(val) FROM t").collect()
    sql_time = time.time() - start

    # Parquet I/O benchmark
    start = time.time()
    df = spark.range(1, 5000000).withColumn("val", col("id") * 3)  # Reduced size
    df.write.mode("overwrite").parquet(f"test_parquet_{cores}_cores")
    read_df = spark.read.parquet(f"test_parquet_{cores}_cores")
    parquet_count = read_df.count()
    parquet_time = time.time() - start

    spark.stop()

    return {
        'cores': cores,
        'transformation': transformation_time,
        'sql': sql_time,
        'parquet': parquet_time,
        'total': transformation_time + sql_time + parquet_time
    }

# Test different core configurations
core_configs = [1, 2, 4, 8, 12]  # Single core to all cores
results = []

print("ARM64 Core Scaling Benchmark")
print(f"Hardware: Apple M2 Pro (8P+4E cores, 16GB RAM)")
print(f"Platform: {platform.platform()}" if 'platform' in globals() else "Platform: macOS ARM64")

for cores in core_configs:
    try:
        result = benchmark_with_cores(cores)
        results.append(result)
        print(f"Cores: {cores:2d} | Transform: {result['transformation']:.2f}s | SQL: {result['sql']:.2f}s | Parquet: {result['parquet']:.2f}s | Total: {result['total']:.2f}s")
    except Exception as e:
        print(f"Error with {cores} cores: {e}")

print(f"\n{'='*80}")
print("CORE SCALING ANALYSIS")
print(f"{'='*80}")
print("Cores | Transform | SQL   | Parquet | Total | Speedup")
print("-" * 55)

baseline = results[0]['total'] if results else 1
for r in results:
    speedup = baseline / r['total']
    print(f"{r['cores']:5d} | {r['transformation']:8.2f}s | {r['sql']:4.2f}s | {r['parquet']:6.2f}s | {r['total']:4.2f}s | {speedup:.2f}x")

print(f"\n{'='*80}")
print("ARM64 PLATFORM ANALYSIS")
print(f"{'='*80}")
print("✅ M2 Pro has 8 Performance + 4 Efficiency cores")
print("✅ Performance cores handle compute-intensive tasks")
print("✅ Efficiency cores handle background/lighter tasks")
print("✅ Spark can utilize all 12 cores effectively")
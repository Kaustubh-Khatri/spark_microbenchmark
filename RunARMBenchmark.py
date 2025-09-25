from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("ARM Benchmark Python") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

def benchmark(name):
    """Decorator function to time operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            print(f"{name} took {end - start:.9f} seconds")
            return result
        return wrapper
    return decorator

print("Starting ARM Performance Benchmark Suite")
print(f"Spark version: {spark.version}")
print(f"Python version: {spark.sparkContext.pythonVer}")
print(f"Available cores: {spark.sparkContext.defaultParallelism}")

# (a) Transformation Benchmark
print("\n" + "=" * 80)
print("(a) Transformation Benchmark")
print("=" * 80)

@benchmark("Python Transformation")
def transformation_benchmark():
    data = spark.range(1, 100000000)
    result = data.filter(col("id") % 2 == 0).count()
    print(f"Count: {result}")
    return result

transformation_benchmark()

# (b) SQL Query Benchmark
print("\n" + "=" * 80)
print("(b) SQL Query Benchmark")
print("=" * 80)

@benchmark("Python SQL Query")
def sql_query_benchmark():
    df = spark.range(1, 50000000).withColumn("val", col("id") * 2)
    df.createOrReplaceTempView("t")
    spark.sql("SELECT avg(val) FROM t").show()

sql_query_benchmark()

# (c) Parquet I/O Benchmark
print("\n" + "=" * 80)
print("(c) Parquet I/O Benchmark")
print("=" * 80)

@benchmark("Python Parquet I/O")
def parquet_io_benchmark():
    df = spark.range(1, 10000000).withColumn("val", col("id") * 3)
    df.write.mode("overwrite").parquet("test_parquet_python")
    read_df = spark.read.parquet("test_parquet_python")
    count = read_df.count()
    print(f"Parquet Count: {count}")
    return count

parquet_io_benchmark()

print("\n" + "=" * 80)
print("Benchmark Suite Completed Successfully")
print("=" * 80)

spark.stop()
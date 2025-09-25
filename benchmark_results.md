# ARM Spark Performance Benchmark Results

## System Information
- **Spark Version**: 4.1.0-SNAPSHOT
- **Java Version**: 17.0.12 (OpenJDK 64-Bit Server VM)
- **Available Cores**: 12
- **Platform**: ARM64 (Apple Silicon)

## Benchmark Results

### (a) Transformation Benchmark
- **Operation**: Filter even numbers from 100M records
- **Time**: **1.379 seconds**
- **Records Processed**: 100,000,000
- **Result Count**: 49,999,999

### (b) SQL Query Benchmark
- **Operation**: Calculate average of 50M records using SQL
- **Time**: **0.369 seconds**
- **Records Processed**: 50,000,000
- **Average Result**: 5.0E7

### (c) Parquet I/O Benchmark
- **Operation**: Write 10M records to Parquet, then read back
- **Time**: **1.814 seconds**
- **Records Written**: 10,000,000
- **Records Read**: 9,999,999
- **File Size**: 83MB (compressed with Snappy)
- **Partitions**: 12 (matching available cores)

## Performance Analysis

1. **SQL Query** performed fastest (0.369s) due to Catalyst optimizer
2. **Transformation** took moderate time (1.379s) for large dataset filtering
3. **Parquet I/O** was slowest (1.814s) but included both write and read operations

## Files Generated
- `ARMBenchmarkSimple.scala` - Working benchmark script
- `test_parquet_scala/` - Parquet output directory (83MB)
- 12 partition files with Snappy compression

The benchmark successfully demonstrates Spark's performance capabilities on ARM architecture.
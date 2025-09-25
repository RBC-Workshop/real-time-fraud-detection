# Detailed Testing Instructions: Scala vs PySpark Side-by-Side Comparison

This document provides step-by-step instructions for running both the original Scala and converted PySpark fraud detection pipelines side-by-side to validate identical functionality.

## Prerequisites

### Infrastructure Setup
1. **Cassandra**: Running on localhost:9042 with keyspace `creditcard`
2. **Kafka**: Running on localhost:9092 with topic `creditcardTransaction` (3 partitions)
3. **Spark**: Standalone cluster or local mode
4. **Data**: Customer and transaction datasets in place

### Create Kafka Topic
```bash
kafka-topics --zookeeper localhost:2181 --create --topic creditcardTransaction --replication-factor 1 --partitions 3
```

### Cassandra Schema Setup
```sql
CREATE KEYSPACE IF NOT EXISTS creditcard WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE creditcard;

CREATE TABLE IF NOT EXISTS customer (
    cc_num text PRIMARY KEY,
    first text,
    last text,
    gender text,
    street text,
    city text,
    state text,
    zip text,
    lat double,
    long double,
    job text,
    dob text
);

CREATE TABLE IF NOT EXISTS fraud_transaction (
    cc_num text,
    trans_time text,
    trans_num text,
    category text,
    merchant text,
    amt double,
    merch_lat double,
    merch_long double,
    distance double,
    age int,
    is_fraud double,
    PRIMARY KEY (cc_num, trans_time)
);

CREATE TABLE IF NOT EXISTS non_fraud_transaction (
    cc_num text,
    trans_time text,
    trans_num text,
    category text,
    merchant text,
    amt double,
    merch_lat double,
    merch_long double,
    distance double,
    age int,
    is_fraud double,
    PRIMARY KEY (cc_num, trans_time)
);
```

## Step 1: Build and Prepare Both Versions

### Build Scala Version
```bash
cd "Fraud Detection"
mvn clean package
# This creates: target/fruaddetection-spark.jar
```

### Prepare PySpark Version
```bash
cd ../
pip install -r requirements.txt
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

## Step 2: Data Import and ML Training

### Import Initial Data (Scala)
```bash
cd "Fraud Detection"
spark-submit \
  --class com.datamantra.spark.jobs.IntialImportToCassandra \
  --name "Import Data to Cassandra" \
  --master local[*] \
  target/fruaddetection-spark.jar \
  src/main/resources/application-local.conf
```

### Train ML Models (Scala)
```bash
spark-submit \
  --class com.datamantra.spark.jobs.FraudDetectionTraining \
  --name "Fraud Detection Spark ML Training" \
  --master local[*] \
  target/fruaddetection-spark.jar \
  src/main/resources/application-local.conf
```

## Step 3: Start Kafka Producer

### Start Transaction Producer
```bash
cd "../Creditcard Producer"
# Build and run the Kafka producer to generate test transactions
mvn clean package
java -cp target/classes:target/dependency/* com.datamantra.producer.TransactionProducer
```

## Step 4: Run Both Streaming Jobs Side-by-Side

### Terminal 1: Run Scala Structured Streaming
```bash
cd "Fraud Detection"
spark-submit \
  --class com.datamantra.spark.jobs.RealTimeFraudDetection.StructuredStreamingFraudDetection \
  --name "Scala Fraud Detection Structured Streaming" \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/scala-checkpoint \
  target/fruaddetection-spark.jar \
  src/main/resources/application-local.conf
```

### Terminal 2: Run PySpark Structured Streaming
```bash
cd ../
spark-submit \
  --name "PySpark Fraud Detection Structured Streaming" \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/pyspark-checkpoint \
  --py-files pyspark_fraud_detection.zip \
  pyspark_fraud_detection/jobs/structured_streaming_fraud_detection.py
```

## Step 5: Data Comparison and Validation

### Monitor Real-time Processing
```bash
# Terminal 3: Monitor Kafka consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "RealTime Creditcard FraudDetection"

# Terminal 4: Monitor Cassandra data ingestion
cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction;"
cqlsh -e "SELECT COUNT(*) FROM creditcard.non_fraud_transaction;"
```

### Compare Output Data

#### 1. Record Counts Comparison
```bash
# Check fraud transaction counts
echo "Scala fraud transactions:"
cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction WHERE trans_time >= '$(date -d '5 minutes ago' '+%Y-%m-%d %H:%M:%S')';"

echo "PySpark fraud transactions:"
cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction WHERE trans_time >= '$(date -d '5 minutes ago' '+%Y-%m-%d %H:%M:%S')';"
```

#### 2. Data Format Validation
```bash
# Compare sample records structure
cqlsh -e "SELECT * FROM creditcard.fraud_transaction LIMIT 5;" > scala_fraud_sample.txt
cqlsh -e "SELECT * FROM creditcard.non_fraud_transaction LIMIT 5;" > scala_non_fraud_sample.txt

# After running PySpark version, compare the same queries
# The data format, column types, and values should be identical
```

#### 3. Distance Calculation Verification
```bash
# Verify distance calculations are identical
cqlsh -e "SELECT cc_num, merch_lat, merch_long, distance FROM creditcard.fraud_transaction WHERE distance IS NOT NULL LIMIT 10;" > distance_comparison.txt

# Check that distance values match between versions for same transactions
```

#### 4. ML Prediction Consistency
```bash
# Compare prediction distributions
cqlsh -e "SELECT is_fraud, COUNT(*) FROM creditcard.fraud_transaction GROUP BY is_fraud;"
cqlsh -e "SELECT is_fraud, COUNT(*) FROM creditcard.non_fraud_transaction GROUP BY is_fraud;"

# Verify fraud/non-fraud classification ratios are similar
```

## Step 6: Performance Comparison

### Monitor Resource Usage
```bash
# Terminal 5: Monitor system resources
top -p $(pgrep -f "StructuredStreamingFraudDetection")
top -p $(pgrep -f "structured_streaming_fraud_detection.py")
```

### Throughput Measurement
```bash
# Measure processing throughput
# Record timestamps and counts every minute for 10 minutes
for i in {1..10}; do
  echo "Minute $i:"
  cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction;"
  cqlsh -e "SELECT COUNT(*) FROM creditcard.non_fraud_transaction;"
  sleep 60
done
```

## Step 7: Validation Checklist

### ✅ Data Consistency Checks
- [ ] **Record counts match**: Same number of fraud/non-fraud transactions processed
- [ ] **Schema compatibility**: All columns present with correct data types
- [ ] **Distance calculations**: Haversine formula produces identical results (±0.01 km tolerance)
- [ ] **ML predictions**: Similar fraud detection rates and classification accuracy
- [ ] **Timestamp handling**: Transaction times processed correctly
- [ ] **Kafka offset management**: No duplicate or missing messages

### ✅ Performance Validation
- [ ] **Throughput**: PySpark processes similar number of records per second
- [ ] **Memory usage**: Comparable memory footprint
- [ ] **CPU utilization**: Similar processing efficiency
- [ ] **Latency**: End-to-end processing time within acceptable range

### ✅ Error Handling
- [ ] **Graceful shutdown**: Both versions stop cleanly with Ctrl+C
- [ ] **Fault tolerance**: Recovery from temporary Kafka/Cassandra outages
- [ ] **Invalid data**: Proper handling of malformed JSON messages

## Step 8: Cleanup

### Stop All Processes
```bash
# Stop streaming jobs with Ctrl+C in respective terminals
# Stop Kafka producer
# Verify no zombie processes remain
ps aux | grep -E "(spark|kafka|cassandra)"
```

### Clean Checkpoint Directories
```bash
rm -rf /tmp/scala-checkpoint
rm -rf /tmp/pyspark-checkpoint
```

## Expected Results

### Success Criteria
1. **Identical data output**: Same records in fraud/non-fraud tables
2. **Consistent predictions**: ML model produces same classifications
3. **Accurate calculations**: Distance computations match within tolerance
4. **Similar performance**: Throughput within 10% of Scala version
5. **No data loss**: All Kafka messages processed exactly once

### Common Issues and Solutions

#### Issue: Import errors in PySpark
```bash
# Solution: Ensure PYTHONPATH is set correctly
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

#### Issue: ML model loading fails
```bash
# Solution: Verify model paths in configuration
ls -la src/main/resources/spark/training/
```

#### Issue: Cassandra connection timeout
```bash
# Solution: Check Cassandra is running and accessible
cqlsh -e "DESCRIBE KEYSPACES;"
```

#### Issue: Different distance calculations
```bash
# Solution: Verify coordinate precision and rounding
# Both versions should use same precision (2 decimal places)
```

## Automated Comparison Script

Create `compare_outputs.sh`:
```bash
#!/bin/bash
# Automated comparison script
echo "Comparing Scala vs PySpark outputs..."

# Wait for both systems to process data
sleep 300

# Compare record counts
SCALA_FRAUD=$(cqlsh -e "SELECT COUNT(*) FROM creditcard.fraud_transaction;" | grep -o '[0-9]*')
SCALA_NON_FRAUD=$(cqlsh -e "SELECT COUNT(*) FROM creditcard.non_fraud_transaction;" | grep -o '[0-9]*')

echo "Scala - Fraud: $SCALA_FRAUD, Non-fraud: $SCALA_NON_FRAUD"
echo "Total processed: $((SCALA_FRAUD + SCALA_NON_FRAUD))"

# Add similar checks for data consistency, distance calculations, etc.
```

This comprehensive testing approach ensures that the PySpark conversion maintains identical functionality to the original Scala implementation while providing measurable validation criteria.

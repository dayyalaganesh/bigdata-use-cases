# Smart Factory IoT Analytics

**PySpark + Parquet + HDFS + Hadoop MapReduce Streaming + YARN**

## 1. Project Overview

This end-to-end lab uses **one Smart Factory sensor dataset** to demonstrate two processing approaches:

1. **PySpark + Parquet** for analytics.
2. **Hadoop MapReduce Streaming** using CSV/Text input.

Both paths originate from the same sensor data, and HDFS stores the input and output data.

### Final architecture

```text
                    SMART FACTORY SENSOR DATA
                              |
                              v
                           PySpark
                              |
                 +------------+------------+
                 |                         |
                 v                         v
              Parquet                   CSV/Text
                 |                         |
                 v                         v
          PySpark Analytics       Hadoop Streaming
                 |                         |
                 |                     Mapper
                 |                         |
                 |                   Shuffle/Sort
                 |                         |
                 |                     Reducer
                 |                         |
                 v                         v
          Parquet Output             Text Output
                 |                         |
                 +------------+------------+
                              |
                              v
                             HDFS
```

---

# 2. What You Will Learn

By completing this lab, you will practice:

- Creating sensor data with Python/Pandas.
- Storing data in Parquet format.
- Creating HDFS directories.
- Uploading data to HDFS.
- Reading HDFS data with PySpark.
- Performing `groupBy()` and aggregations.
- Writing PySpark results as Parquet.
- Writing CSV/Text from PySpark.
- Understanding Hadoop Streaming.
- Creating a Mapper and Reducer in Python.
- Understanding Mapper → Shuffle/Sort → Reducer.
- Running MapReduce through YARN.
- Verifying results in HDFS.
- Understanding the difference between modern PySpark analytics and traditional MapReduce.

---

# 3. Prerequisites

Check Java:

```bash
java -version
```

Check Hadoop:

```bash
hadoop version
```

Check PySpark:

```bash
pyspark --version
```

Check Hadoop processes:

```bash
jps
```

Expected processes:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

Start Hadoop if necessary:

```bash
sudo service ssh start
start-dfs.sh
start-yarn.sh
```

---

# 4. MapReduce Configuration

Open:

```bash
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

Make sure it contains:

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

</configuration>
```

Verify:

```bash
cat $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

The important setting is:

```xml
<name>mapreduce.framework.name</name>
<value>yarn</value>
```

This configures MapReduce to use YARN.

---

# 5. Project Directory

Create the project directory:

```bash
mkdir -p ~/smart-factory-pyspark
```

Enter it:

```bash
cd ~/smart-factory-pyspark
```

Check:

```bash
pwd
```

Example:

```text
/home/ganesh/smart-factory-pyspark
```

> Replace `/home/ganesh` with your own Linux home directory if necessary.

---

# 6. Local Folder Structure

The final local project contains:

```text
/home/ganesh/smart-factory-pyspark/
│
├── create_factory_data.py
├── factory_sensor_data.parquet
├── factory_sensor_analysis.py
├── mapper.py
└── reducer.py
```

---

# 7. Create the Sensor Dataset

Create the Python program:

```bash
nano create_factory_data.py
```

Add:

```python
import pandas as pd

data = {
    "machine_id": [
        "M001", "M002", "M003",
        "M004", "M005", "M006",
        "M007", "M008", "M009"
    ],

    "plant": [
        "Plant-A", "Plant-A", "Plant-A",
        "Plant-B", "Plant-B", "Plant-B",
        "Plant-C", "Plant-C", "Plant-C"
    ],

    "sensor_type": [
        "Temperature", "Temperature", "Temperature",
        "Temperature", "Temperature", "Temperature",
        "Temperature", "Temperature", "Temperature"
    ],

    "temperature": [
        72.5, 75.0, 80.5,
        65.0, 68.5, 70.0,
        85.5, 88.0, 90.5
    ],

    "pressure": [
        30.2, 31.0, 32.1,
        28.2, 29.0, 29.5,
        34.0, 35.2, 36.0
    ],

    "vibration": [
        2.1, 2.3, 3.0,
        1.8, 2.0, 2.2,
        3.5, 3.8, 4.1
    ]
}

df = pd.DataFrame(data)

df.to_parquet(
    "factory_sensor_data.parquet",
    engine="pyarrow",
    index=False
)

print("Factory sensor Parquet file created successfully")
print(df)
```

Run:

```bash
python3 create_factory_data.py
```

Check:

```bash
ls -lh
```

Check the Parquet schema:

```bash
python3 -c "import pyarrow.parquet as pq; print(pq.read_schema('factory_sensor_data.parquet'))"
```

Expected columns:

```text
machine_id
plant
sensor_type
temperature
pressure
vibration
```

---

# 8. Create HDFS Directories

Create the sensor input directory:

```bash
hdfs dfs -mkdir -p /smart-factory/sensors/input
```

Create the sensor output directory:

```bash
hdfs dfs -mkdir -p /smart-factory/sensors/output
```

Create the MapReduce input directory:

```bash
hdfs dfs -mkdir -p /smart-factory/mapreduce/input
```

The final HDFS structure will be:

```text
/smart-factory/
│
├── sensors/
│   ├── input/
│   └── output/
│
└── mapreduce/
    ├── input/
    └── output/
```

Check:

```bash
hdfs dfs -ls -R /smart-factory
```

---

# 9. Upload the Sensor Dataset to HDFS

Upload the Parquet file:

```bash
hdfs dfs -put -f factory_sensor_data.parquet /smart-factory/sensors/input/
```

Check:

```bash
hdfs dfs -ls -h /smart-factory/sensors/input
```

Expected:

```text
factory_sensor_data.parquet
```

The data now exists in HDFS:

```text
Local
/home/ganesh/smart-factory-pyspark/
        |
        | factory_sensor_data.parquet
        |
        v
HDFS
/smart-factory/sensors/input/
        |
        └── factory_sensor_data.parquet
```

---

# 10. Create the Main PySpark Program

Create:

```bash
nano factory_sensor_analysis.py
```

Add:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max

# Create Spark session
spark = SparkSession.builder \
    .appName("Smart Factory Sensor Analytics") \
    .getOrCreate()

# Read Parquet from HDFS
df = spark.read.parquet(
    "/smart-factory/sensors/input"
)

print("INPUT DATA")
df.show()

print("INPUT SCHEMA")
df.printSchema()

# Plant-wise analysis
result = df.groupBy("plant").agg(
    count("*").alias("reading_count"),
    avg("temperature").alias("average_temperature"),
    max("temperature").alias("maximum_temperature")
)

print("PLANT-WISE TEMPERATURE ANALYSIS")

result.orderBy("plant").show()

# Write analysis result as Parquet
result.write \
    .mode("overwrite") \
    .parquet(
        "/smart-factory/sensors/output"
    )

print("Parquet analysis completed")

# Create CSV/Text data for MapReduce Streaming
streaming_df = df.select(
    "plant",
    "machine_id",
    "temperature"
)

streaming_df.write \
    .mode("overwrite") \
    .option("header", "false") \
    .option("sep", ",") \
    .csv(
        "/smart-factory/mapreduce/input"
    )

print("CSV data created for MapReduce Streaming")

print("All processing completed successfully")

spark.stop()
```

Spark supports reading and writing Parquet through Spark SQL, including Parquet datasets stored in HDFS. citeturn0search1

---

# 11. Remove Previous Outputs

Before rerunning the Spark job:

```bash
hdfs dfs -rm -r -f /smart-factory/sensors/output
```

Remove the previous MapReduce input:

```bash
hdfs dfs -rm -r -f /smart-factory/mapreduce/input
```

Recreate it:

```bash
hdfs dfs -mkdir -p /smart-factory/mapreduce/input
```

The original Parquet input remains:

```text
/smart-factory/sensors/input/factory_sensor_data.parquet
```

---

# 12. Run PySpark on YARN

Run:

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    factory_sensor_analysis.py
```

The program now does two things from the same DataFrame.

### Path 1 — PySpark analytics

```text
HDFS Parquet
     |
     v
PySpark DataFrame
     |
     v
groupBy + aggregation
     |
     v
Parquet output
     |
     v
/smart-factory/sensors/output
```

### Path 2 — MapReduce input

```text
Same PySpark DataFrame
     |
     v
CSV/Text
     |
     v
/smart-factory/mapreduce/input
```

---

# 13. Check PySpark Parquet Output

Run:

```bash
hdfs dfs -ls /smart-factory/sensors/output
```

You should see files similar to:

```text
part-00000-xxxxxxxx.parquet
_SUCCESS
```

Spark generally writes a Parquet dataset as a directory containing part files and metadata rather than as one ordinary file.

Check its size:

```bash
hdfs dfs -du -h /smart-factory/sensors/output
```

---

# 14. Check CSV/Text Generated by PySpark

Run:

```bash
hdfs dfs -ls /smart-factory/mapreduce/input
```

You should see something similar to:

```text
part-00000-xxxxxxxx.csv
_SUCCESS
```

Read the CSV:

```bash
hdfs dfs -cat /smart-factory/mapreduce/input/part-*.csv
```

Expected:

```text
Plant-A,M001,72.5
Plant-A,M002,75.0
Plant-A,M003,80.5
Plant-B,M004,65.0
Plant-B,M005,68.5
Plant-B,M006,70.0
Plant-C,M007,85.5
Plant-C,M008,88.0
Plant-C,M009,90.5
```

Important:

The CSV is generated from the **same DataFrame** that PySpark read from the original sensor Parquet data.

---

# 15. Create the MapReduce Mapper

Create:

```bash
nano mapper.py
```

Add:

```python
import sys

for line in sys.stdin:

    line = line.strip()

    if not line:
        continue

    fields = line.split(",")

    plant = fields[0]
    temperature = fields[2]

    print(f"{plant}\t{temperature}")
```

Make it executable:

```bash
chmod +x mapper.py
```

### Mapper purpose

The input:

```text
Plant-A,M001,72.5
```

becomes:

```text
Plant-A    72.5
```

The Mapper extracts:

```text
key   = plant
value = temperature
```

---

# 16. Create the MapReduce Reducer

Create:

```bash
nano reducer.py
```

Add:

```python
import sys

current_plant = None
temperatures = []

for line in sys.stdin:

    line = line.strip()

    if not line:
        continue

    plant, temperature = line.split("\t")

    temperature = float(temperature)

    if current_plant == plant:

        temperatures.append(temperature)

    else:

        if current_plant is not None:

            print(
                current_plant,
                len(temperatures),
                sum(temperatures) / len(temperatures),
                max(temperatures),
                sep="\t"
            )

        current_plant = plant
        temperatures = [temperature]


if current_plant is not None:

    print(
        current_plant,
        len(temperatures),
        sum(temperatures) / len(temperatures),
        max(temperatures),
        sep="\t"
    )
```

Make it executable:

```bash
chmod +x reducer.py
```

---

# 17. Test the Mapper

Read the CSV from HDFS:

```bash
hdfs dfs -cat /smart-factory/mapreduce/input/part-*.csv
```

Send it into the Mapper:

```bash
hdfs dfs -cat /smart-factory/mapreduce/input/part-*.csv | ./mapper.py
```

Expected:

```text
Plant-A    72.5
Plant-A    75.0
Plant-A    80.5
Plant-B    65.0
Plant-B    68.5
Plant-B    70.0
Plant-C    85.5
Plant-C    88.0
Plant-C    90.5
```

This is the conceptual **Map output**.

---

# 18. Test Mapper + Shuffle/Sort + Reducer Locally

Run:

```bash
hdfs dfs -cat /smart-factory/mapreduce/input/part-*.csv | ./mapper.py | sort | ./reducer.py
```

Expected:

```text
Plant-A    3    76.0    80.5
Plant-B    3    67.83333333333333    70.0
Plant-C    3    88.0    90.5
```

Conceptually:

```text
CSV
 |
 v
Mapper
 |
 v
Plant-A    72.5
Plant-A    75.0
Plant-A    80.5
 |
 v
Shuffle + Sort
 |
 v
Plant-A -> 72.5, 75.0, 80.5
 |
 v
Reducer
 |
 v
count = 3
average = 76.0
maximum = 80.5
```

---

# 19. Run the Actual Hadoop Streaming Job

Remove any previous output:

```bash
hdfs dfs -rm -r -f /smart-factory/mapreduce/output
```

Run Hadoop Streaming:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-input /smart-factory/mapreduce/input \
-output /smart-factory/mapreduce/output \
-mapper mapper.py \
-reducer reducer.py \
-file mapper.py \
-file reducer.py
```

YARN manages the MapReduce application's resources.

---

# 20. Check MapReduce Output

Run:

```bash
hdfs dfs -ls /smart-factory/mapreduce/output
```

Expected:

```text
_SUCCESS
part-00000
```

Read the result:

```bash
hdfs dfs -cat /smart-factory/mapreduce/output/part-00000
```

Expected:

```text
Plant-A    3    76.0    80.5
Plant-B    3    67.83333333333333    70.0
Plant-C    3    88.0    90.5
```

---

# 21. Verify PySpark Parquet Result

Start PySpark:

```bash
pyspark
```

Inside PySpark:

```python
result = spark.read.parquet(
    "/smart-factory/sensors/output"
)
```

Display:

```python
result.show()
```

Expected:

```text
+-------+------------+-------------------+------------------+
|  plant|reading_count|average_temperature|maximum_temperature|
+-------+------------+-------------------+------------------+
|Plant-A|           3|               76.0|              80.5|
|Plant-B|           3|  67.83333333333333|              70.0|
|Plant-C|           3|               88.0|              90.5|
+-------+------------+-------------------+------------------+
```

Check the schema:

```python
result.printSchema()
```

Expected:

```text
root
 |-- plant: string
 |-- reading_count: long
 |-- average_temperature: double
 |-- maximum_temperature: double
```

Exit:

```python
exit()
```

---

# 22. Final HDFS Structure

Run:

```bash
hdfs dfs -ls -R /smart-factory
```

Conceptually:

```text
/smart-factory
│
├── sensors
│   │
│   ├── input
│   │   └── factory_sensor_data.parquet
│   │
│   └── output
│       ├── part-00000-xxxxxxxx.parquet
│       └── _SUCCESS
│
└── mapreduce
    │
    ├── input
    │   ├── part-00000-xxxxxxxx.csv
    │   └── _SUCCESS
    │
    └── output
        ├── part-00000
        └── _SUCCESS
```

---

# 23. Final Local Structure

```text
/home/ganesh/smart-factory-pyspark/
│
├── create_factory_data.py
├── factory_sensor_data.parquet
├── factory_sensor_analysis.py
├── mapper.py
└── reducer.py
```

---

# 24. Complete Data Flow

```text
                 SENSOR DATA
                     |
                     v
              Python / Pandas
                     |
                     v
              Parquet Dataset
                     |
                     v
                    HDFS
                     |
                     v
              PySpark reads Parquet
                     |
             +-------+-------+
             |               |
             v               v
       PySpark Analysis    CSV/Text
             |               |
             v               v
       Parquet Output    MapReduce
                             |
                         Mapper
                             |
                       Shuffle / Sort
                             |
                          Reducer
                             |
                             v
                       Text Output
```

---

# 25. What Each Technology Does

| Component | Role |
|---|---|
| Python/Pandas | Creates the initial sensor dataset |
| Parquet | Efficient columnar storage format |
| HDFS | Distributed storage layer |
| PySpark | Distributed data processing and analytics |
| CSV/Text | Simple input format for Hadoop Streaming |
| Mapper | Converts each record into `plant → temperature` |
| Shuffle/Sort | Groups Mapper output by key |
| Reducer | Calculates count, average, and maximum |
| YARN | Manages cluster resources and applications |
| `mapred-site.xml` | Configures MapReduce to use YARN |

---

# 26. Interview Explanation

You can explain the project like this:

> "I created a Smart Factory IoT dataset containing machine, plant, temperature, pressure, and vibration readings. I stored the sensor data as Parquet and uploaded it to HDFS. PySpark read the Parquet data from HDFS and performed plant-wise temperature analysis. From the same DataFrame, I also generated CSV data for Hadoop Streaming. I then used a Python Mapper and Reducer to calculate the count, average, and maximum temperature for each plant. The MapReduce job was executed through YARN, and both the PySpark and MapReduce results were stored in HDFS."

---

# 27. Key Concept

The most important idea in this lab is:

```text
ONE SENSOR DATASET
       |
       v
      HDFS
       |
       v
    PySpark
       |
       +-------------------+
       |                   |
       v                   v
 PySpark Analytics      CSV/Text
       |                   |
       v                   v
   Parquet             MapReduce
    Output                |
       |                  v
       |               Reducer
       |                  |
       +--------+---------+
                |
                v
               HDFS
```

This lets you compare two Hadoop ecosystem processing approaches using the **same underlying data**:

- **PySpark** → DataFrame-based distributed analytics.
- **MapReduce Streaming** → Mapper/Shuffle/Reducer processing.

---

# 28. Useful Verification Commands

### Check HDFS

```bash
hdfs dfs -ls -R /smart-factory
```

### Check sensor input

```bash
hdfs dfs -ls /smart-factory/sensors/input
```

### Check PySpark output

```bash
hdfs dfs -ls /smart-factory/sensors/output
```

### Check MapReduce input

```bash
hdfs dfs -ls /smart-factory/mapreduce/input
```

### Read MapReduce input

```bash
hdfs dfs -cat /smart-factory/mapreduce/input/part-*.csv
```

### Check MapReduce output

```bash
hdfs dfs -ls /smart-factory/mapreduce/output
```

### Read MapReduce output

```bash
hdfs dfs -cat /smart-factory/mapreduce/output/part-00000
```

### Check Hadoop processes

```bash
jps
```

---

# 29. Final Summary

```text
Python/Pandas
     ↓
Sensor Dataset
     ↓
Parquet
     ↓
HDFS
     ↓
PySpark
     ↓
     +--------------------------+
     |                          |
     v                          v
Analytics                  CSV/Text
     |                          |
     v                          v
Parquet Output             Mapper
                                |
                            Shuffle/Sort
                                |
                             Reducer
                                |
                                v
                           Text Output
                                |
                                v
                               HDFS
```

The lab demonstrates an end-to-end Big Data workflow involving **Python/Pandas, Parquet, HDFS, PySpark, Hadoop Streaming, MapReduce, and YARN**.

import sys
from pyspark.sql import SparkSession

if len(sys.argv) < 2:
    print("Usage: sql_runner.py <sql_query>")
    sys.exit(1)

sql = sys.argv[1]

spark = SparkSession.builder.appName("lake-sql").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.sql(sql)
    df.show(500, truncate=False)
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(1)
finally:
    spark.stop()

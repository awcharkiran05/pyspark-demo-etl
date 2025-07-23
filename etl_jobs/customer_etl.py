from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerETL").getOrCreate()

data = [("John", "USA"), ("Amit", "India"), ("Sara", "India")]
df = spark.createDataFrame(data, ["name", "country"])

df_filtered = df.filter(df["country"] == "INDIA")
df_filtered.show()
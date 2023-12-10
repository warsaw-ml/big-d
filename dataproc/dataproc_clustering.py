from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("EmbeddingsClustering").getOrCreate()

# Read Parquet files from GCS
parquet_path = "gs://big-d-project-master-dataset/parquet/*.parquet"

columns_to_select = [
    "embedding"
]

df = spark.read.option("mergeSchema", "true").parquet(parquet_path).select(columns_to_select)

# Extract 'embeddings' column
assembler = VectorAssembler(inputCols=["embedding"], outputCol="features")
df = assembler.transform(df)

# Perform k-means clustering
kmeans = KMeans(k=5, featuresCol="features", predictionCol="cluster")
model = kmeans.fit(df)
clustered_df = model.transform(df)

# Save results to GCS or any other desired location
output_path = "gs://big-d-project-master-dataset/clusters"
clustered_df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()

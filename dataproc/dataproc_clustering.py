from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, ArrayType, FloatType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Create a Spark session
spark = SparkSession.builder.appName("EmbeddingsClustering").getOrCreate()

logger.info("Spark session created")

# Read Parquet files from GCS and filter those with 'embedding' column
parquet_path = "gs://big-d-project-master-dataset/parquet/*.parquet"
df_list = []

for file_path in spark.sparkContext.binaryFiles(parquet_path).keys().collect():
    temp_df = spark.read.option("mergeSchema", "true").parquet(file_path)
    if "embedding" in temp_df.columns:
        df_list.append(temp_df)
        logger.info('append successful')
# Union the filtered dataframes
df = df_list[0]


for i in range(1, len(df_list)):
    df = df.union(df_list[i])

logger.info(f"Length of DataFrame: {len(df_list)}")

df = df.withColumn("embedding_vector", col("embedding").cast("array<float>"))
assembler = VectorAssembler(inputCols=["embedding_vector"], outputCol="features")
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

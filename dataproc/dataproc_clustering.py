from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, size
from pyspark.sql.types import StructType, StructField, ArrayType, FloatType
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import logging


EMBEDDING_SIZE = 768
CSV_PATH = "gs://bda-wut-project-master-dataset/csv/embeddings_clustering.csv"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Spark session
spark = SparkSession.builder.appName("EmbeddingsClustering").getOrCreate()
logger.info("1. Spark session created")

# Read Parquet files from GCS and filter those with 'embedding' column
parquet_path = "gs://bda-wut-project-master-dataset/parquet/*.parquet"
df_list = []

for file_path in spark.sparkContext.binaryFiles(parquet_path).keys().collect():
    temp_df = spark.read.option("mergeSchema", "true").parquet(file_path)
    df_list.append(temp_df)

# Perform union
df = df_list[0]
for temp_df in df_list[1:]:
    df = df.unionByName(temp_df, allowMissingColumns=True)

logger.info("2. Parquet files read from GCS")

# drop duplicates, filter records with null embedding and filter records with embedding size != 768
df = df.dropDuplicates()
df = df.filter(col("embedding").isNotNull()).filter(size("embedding") == EMBEDDING_SIZE)

logger.info("3. Duplicates and null values in embeddings dropped")

# select only the message_id and embedding column
list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
embedding = df.select(
    "message_id",
    list_to_vector_udf(df["embedding"]).alias("embedding")
    )

logger.info("4. Embedding column selected")

# Create a KMeans model
k = 5
kmeans = KMeans(k=k, seed=1, featuresCol="embedding", predictionCol="cluster")
model = kmeans.fit(embedding)
predictions = model.transform(embedding)

logger.info("5. KMeans model created and predictions made")

# Save message_id and cluster columns to GCS
predictions.select("message_id", "cluster").write.csv(CSV_PATH, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
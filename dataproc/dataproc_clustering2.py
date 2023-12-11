from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Initialize Spark session
spark = SparkSession.builder.appName('KMeansExample').getOrCreate()

# Load JSON files into a DataFrame
data = spark.read.json('gs://big-d-project-master-dataset/telegram/*/*/*.json')

# Assuming 'embedding' is a column containing vectors
vector_assembler = VectorAssembler(inputCols=['embedding'], outputCol='features')
data = vector_assembler.transform(data)

# Perform KMeans clustering
kmeans = KMeans(k=5, featuresCol='features', predictionCol='cluster')
model = kmeans.fit(data)
result = model.transform(data)

# Show the clustering result
result.select('features', 'cluster').show()

# Save the result back to Cloud Storage or any other storage system
result.write.json('gs://big-d-project-master-dataset/clusters_json')

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Extract and explode Hashtags into individual tags
hashtags_df = posts_df.withColumn("hashtag", explode(split(col("Hashtags"), ",")))

# Trim whitespace around hashtags (if any) and count frequency of each hashtag
hashtag_counts = hashtags_df.withColumn("hashtag", col("hashtag").alias("hashtag").rlike("^\s*(\S+)\s*$"))\
    .groupBy("hashtag").count()

# Sort by frequency in descending order and take top 10
top_hashtags = hashtag_counts.orderBy(col("count").desc()).limit(10)

# Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize Sentiment: Positive (> 0.3), Neutral (-0.3 to 0.3), Negative (< -0.3)
posts_df = posts_df.withColumn("SentimentCategory", when(col("SentimentScore") > 0.3, "Positive")
                              .when((col("SentimentScore") <= 0.3) & (col("SentimentScore") >= -0.3), "Neutral")
                              .otherwise("Negative"))

# Group by SentimentCategory and calculate average likes and retweets
sentiment_stats = posts_df.groupBy("SentimentCategory").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
)

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)

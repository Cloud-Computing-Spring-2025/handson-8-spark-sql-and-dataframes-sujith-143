from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join posts and users dataframes on UserID
merged_df = posts_df.join(users_df, on="UserID", how="inner")

# Group by AgeGroup and calculate the average likes and retweets
engagement_df = merged_df.groupBy("AgeGroup").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
)

# Rank engagement by AgeGroup (You can add other metrics to define ranking if needed)
engagement_df = engagement_df.orderBy(col("Avg_Likes").desc(), col("Avg_Retweets").desc())

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)

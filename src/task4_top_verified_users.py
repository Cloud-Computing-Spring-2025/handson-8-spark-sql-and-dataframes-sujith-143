from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join the posts and users datasets on UserID
joined_df = posts_df.join(users_df, on="UserID", how="inner")

# Filter for verified users
verified_users_df = joined_df.filter(col("Verified") == True)

# Calculate the total reach (Likes + Retweets) for each user
verified_users_reach = verified_users_df.groupBy("Username").agg(
    _sum("Likes").alias("Total_Likes"),
    _sum("Retweets").alias("Total_Retweets")
)

# Calculate total reach by summing Likes and Retweets
verified_users_reach = verified_users_reach.withColumn(
    "Total_Reach", col("Total_Likes") + col("Total_Retweets")
)

# Sort by Total Reach in descending order and take top 5
top_verified = verified_users_reach.orderBy(col("Total_Reach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)

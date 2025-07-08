# Databricks notebook source
import requests
from pyspark.sql.types import *
from pyspark.sql import Row

API_KEY = "YOUR_API_KEY"
REGION_CODE = "IN"
MAX_RESULTS = 50

url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={REGION_CODE}&maxResults={MAX_RESULTS}&key={API_KEY}"

response = requests.get(url)

data = response.json()

if "items" not in data:
    print(" Error fetching data:", data.get("error", "Unknown error"))
else:
    
    rows = []
    for item in data["items"]:
        snippet = item["snippet"]
        stats = item["statistics"]
        rows.append(Row(
            video_id=item["id"],
            title=snippet["title"],
            channel_title=snippet["channelTitle"],
            category_id=int(snippet["categoryId"]),
            publish_time=snippet["publishedAt"],
            tags="|".join(snippet.get("tags", [])),
            views=int(stats.get("viewCount", 0)),
            likes=int(stats.get("likeCount", 0)),
            comments=int(stats.get("commentCount", 0))
        ))

    
    df = spark.createDataFrame(rows)

    
    df.createOrReplaceTempView("youtube")
    df.show(50)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Channels by Total Views

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT channel_title,SUM(Views) as Total_Views FROM youtube GROUP BY channel_title ORDER BY Total_Views DESC LIMIT 10 

# COMMAND ----------

# MAGIC %md
# MAGIC Most Liked Videos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title,channel_title,likes FROM youtube ORDER BY likes DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Most Commented Videos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title,comments FROM youtube ORDER BY comments DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Engagement Score (likes + comments)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title, channel_title, (likes + comments) AS engagement_score
# MAGIC FROM youtube
# MAGIC ORDER BY engagement_score DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Like-to-View Ratio (Engagement Rate)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT title,ROUND(likes / views * 100,2) as like_rate FROM youtube WHERE views > 0 ORDER BY like_rate DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Trending Video Count by Category ID

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category_id, COUNT(*) AS video_count
# MAGIC FROM youtube
# MAGIC GROUP BY category_id
# MAGIC ORDER BY video_count DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Video Count by Publish Day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE(publish_time) as publish_date,COUNT(*) as Video_count
# MAGIC FROM youtube
# MAGIC GROUP BY publish_date
# MAGIC ORDER BY publish_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Average Metrics (views, likes, comments)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ROUND(AVG(views)) AS avg_views,
# MAGIC   ROUND(AVG(likes)) AS avg_likes,
# MAGIC   ROUND(AVG(comments)) AS avg_comments
# MAGIC FROM youtube

# COMMAND ----------

# MAGIC %md
# MAGIC Channels with More Than One Trending Video

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT channel_title, COUNT(*) AS video_count
# MAGIC FROM youtube
# MAGIC GROUP BY channel_title
# MAGIC HAVING video_count > 1
# MAGIC ORDER BY video_count DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# 📊 YouTube Trending Video Data Pipeline using PySpark, REST API, and Spark SQL

## 🔍 Overview

This project demonstrates a complete **real-time data pipeline** using **PySpark**, the **YouTube Data API**, and **Spark SQL** in **Databricks**. It extracts trending video data (top 50) from YouTube's REST API, transforms it using PySpark, and performs analytical queries using Spark SQL.

The project is designed for **self-learning** and showcases beginner-to-intermediate-level **data engineering skills**.

---

## ✅ Features

- 📡 **YouTube REST API Integration**  
  Extracts the top 50 trending videos in a specific region (`regionCode=IN`) using a single API call from the YouTube Data API v3.

- 🔄 **PySpark Data Transformation**  
  - Converts JSON response to Spark DataFrame  
  - Casts metrics like views, likes, comments  
  - Adds custom columns like engagement and like ratio

- 📊 **Spark SQL Analytics**  
  Queries the cleaned data using Spark SQL to find the most viewed videos, top channels, popular tags, engagement scores, and more.

- 💻 **Built in Databricks**  
  Entire pipeline runs on the Databricks notebook environment, enabling scalable and interactive big data processing.

---

## 🧰 Tech Stack

- Python 3
- PySpark (Spark DataFrames & SQL)
- YouTube Data API v3 (REST API)
- Databricks

---


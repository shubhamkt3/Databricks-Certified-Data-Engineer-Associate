# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from books

# COMMAND ----------

spark.readStream \
    .table('books') \
    .createOrReplaceTempView('books_streaming_vw')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create or replace TEMP VIEW author_count_vw as (
# MAGIC   select author, count(book_id) as total_books
# MAGIC   from books_streaming_vw
# MAGIC   group by author
# MAGIC )

# COMMAND ----------

spark.table('author_count_vw') \
    .writeStream \
    .trigger(processingTime ='4 seconds') \
    .outputMode('complete') \
    .option('checkpointLocation', 'dbfs:/mnt/demo/author_counts_checkpoint') \
    .table('author_counts')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table('author_count_vw')
    .writeStream
    .trigger(availableNow = True)
    .outputMode('complete')
    .option('checkpointLocation', 'dbfs:/mnt/demo/author_counts_checkpoint')
    .table('author_counts')
    .awaitTermination()
 )

# COMMAND ----------



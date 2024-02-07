# Databricks notebook source
# MAGIC %md 1.Load the  dataset into a PySpark DataFrame.
# MAGIC
# MAGIC

# COMMAND ----------

# the Dataset loaded , Now we create DataFrame 
# And first we needed the necessary labraries imports

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# location of file
%fs ls dbfs:/FileStore/DataBase_Assignment

# COMMAND ----------

#show the file
%fs head dbfs:/FileStore/DataBase_Assignment/database.csv

# COMMAND ----------

#create the DataFrame
earthDF = spark.read.option("header","true").option("inferSchema","true").csv("dbfs:/FileStore/DataBase_Assignment/database.csv")

earthDF.printSchema()
#earthDF.show()


# COMMAND ----------

print(type(earthDF))

# COMMAND ----------

earthDF.display()
earthDF.count()

# COMMAND ----------

# MAGIC %md 2.Convert the Date and Time columns into a timestamp column named Timestamp.
# MAGIC

# COMMAND ----------


earthDF2 = earthDF.select(concat(col("Date"),lit(" "),col("Time")).alias("Timestamp"))
earthDF2.printSchema()

# COMMAND ----------

# MAGIC %md 3.Filter the dataset to include only earthquakes with a magnitude greater than 5.0.
# MAGIC

# COMMAND ----------

earthDF3 = earthDF.where(col("Type")=="Earthquake").filter(col("Magnitude") > 5.0)
earthDF3.display()
earthDF3.count()

# COMMAND ----------

# MAGIC %md 4.Calculate the average depth and magnitude of earthquakes for each earthquake type.
# MAGIC

# COMMAND ----------


earthDF4 = earthDF.groupBy(col("Type")).agg(avg(col("Depth")).alias("avg_depth"),avg(col("Magnitude")).alias("avg_Magnitude"))
earthDF4.display()

# COMMAND ----------

# MAGIC %md 5. Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.
# MAGIC

# COMMAND ----------

def earth_levels(Magnitude):
    if Magnitude < 4.0:
        return "Low"
    elif 4.0 <= Magnitude < 6.0:
        return "Moderate"
    else :
        return "High"
    
 # the UDF with PySpark   
earth_levels_UDF = udf(earth_levels,StringType())

 # UDF to create a new column 'level'
earth_levels_DF = earthDF.withColumn("categorize_earthquakes ", earth_levels_UDF(earthDF["Magnitude"]))    

earth_levels_DF.display()

# COMMAND ----------

# MAGIC %md 6.Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sqrt

# reference location (e.g., (0, 0)).

ref_Latitude = 0.0
ref_Longitude = 0.0

# ADD a new_column for Distance of Each Earthquake
earth_dist_df = earthDF.withColumn("Distance",sqrt((col("Latitude") - ref_Latitude) ** 2 + (col("Longitude")- ref_Longitude) ** 2))
                                   
earth_dist_df.display()                                   

# COMMAND ----------

# MAGIC %md 7.Visualize the geographical distribution of earthquakes on a world map using appropriate libraries (e.g., Basemap or Folium).
# MAGIC

# COMMAND ----------

# The folium labraries for install the Visualize the geographical distribution of earthquake(its a Interactive world map visualization).
import folium

# COMMAND ----------

# collect the Data
earth_data = earthDF.select("ID","Latitude","Longitude").collect()

# folium map centered at a reference location
map_center = [0, 0]
mymap = folium.Map(location=map_center,zoom_start=2)

# markers to add earthquake map

for row in earth_data:
    marker_loc = [row.Latitude,row.Longitude]
    folium.Marker(location=marker_loc, popup = f"earthDF{row.ID}").add_to(mymap)

# mymap is show and save in HTML.file
mymap

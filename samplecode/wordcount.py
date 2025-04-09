from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql import functions as F

datapath = '../exampledata'
datafile = f"{datapath}/wikiOfSpark.txt"


# 创建saprk session 

spark = SparkSession.builder.appName('word count').getOrCreate()

# df = spark.read.format('text').load(datafile)

df = spark.read.text(datafile)

# spark.read.option().csv(csvPath)

# df.show(5)

words_df = df.withColumn('word',explode(split(col("value")," "))).drop('value')

words_df01 = words_df.replace(',','').replace('"','')

# wordcount_df = words_df.groupBy("word").count()

words_df01.show(5)
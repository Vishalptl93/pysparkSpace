from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.master("local").appName("getlatestdate").getOrCreate()
#############################data creation###################################
dataList=[(2345,"2020-12-31","retail","newyork"),(2345,"2021-12-31","other","newyork"),(2345,"2019-11-30","retail","newyork"),(2345,"2018-12-31","other","newyork"),\
          (5678,"2018-12-31","retail","LA"),(5678,"2019-12-31","other","LA"),(5678,"2021-11-30","retail","LA"),(5678,"2019-12-31","other","LA")]
dataSchema=StructType([StructField("loanno",IntegerType()),StructField("asofdate",StringType()),StructField("property",StringType()),StructField("city",StringType())])

df=spark.createDataFrame(dataList,schema=dataSchema).withColumn("asofdate",F.col("asofdate").cast("date"))
df.show()
########################main logic############
win=Window.partitionBy("loanno").orderBy(F.col("asofdate").desc())
df1=df.withColumn("rank",F.row_number().over(win)).where(F.expr("rank==1")).selectExpr("loanno","property as property1")
df1.show()

finaldf=df.join(df1,on=df["loanno"]==df1["loanno"],how="left").drop(df1["loanno"]).withColumn("property",F.col("property1")).drop("property1")
finaldf.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import explode_outer, col, regexp_extract, to_date, weekofyear
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, BooleanType, DateType, LongType, FloatType, ArrayType, StructType
from hdfs import InsecureClient
import requests

def flatten_json(data):
  complex_fields = [
      {
          "field": field.name,
          "type": type(field.dataType)
      }
      for field in data.schema.fields
      if isinstance(field.dataType, (ArrayType, StructType))
  ]

  while len(complex_fields) != 0:
      col_name = complex_fields[0]["field"]

      if complex_fields[0]["type"] == ArrayType:
          data = data.withColumn(col_name, explode_outer(col(col_name)))
      elif complex_fields[0]["type"] == StructType:
          expanded = [col(col_name + "." + k).alias(col_name + "_" + k) for k in [n.name for n in data.select(col_name).schema[0].dataType]]
          data = data.select("*", *expanded).drop(col_name)

      complex_fields = [
          {
              "field": field.name, 
              "type": type(field.dataType)
          } 
          for field in data.schema.fields 
          if isinstance(field.dataType, (ArrayType, StructType))
      ]
  return data


def holobalance_score(spark, client):

    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfScore = dfHolo_final.filter((dfHolo_final.entry_resource_code_coding_code == '89191-1') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id','entry_resource_code_coding_display', 'entry_resource_component_valueQuantity_value', 'Date','Week','Start_time')
    dfScore = dfScore.distinct()
    dfScore = dfScore.withColumnRenamed("entry_resource_component_valueQuantity_value", "Session_score")
    
    dfMeanScore = dfScore.groupBy("Date","entry_resource_code_coding_display").mean("Session_score")
    dfMeanScore = dfMeanScore.withColumnRenamed("avg(entry_resource_component_valueQuantity_value)", "Mean_for_session")
    dfMeanScore = dfMeanScore.sort("Date")
    
    dfMeanScore_weekly = dfScore.groupBy("Week","Date","Patient_id","entry_resource_code_coding_display").agg(mean('Session_score').alias('Mean_of_weekly_score'))
    dfMeanScore_weekly = dfMeanScore_weekly.sort("Week")

    dfSum = dfScore.groupby("Date", "Week","Patient_id").sum("Session_score")
    dfSum = dfSum.withColumnRenamed("sum(Session_score)","Sum_of_session")
    dfSum = dfSum.sort("Date")
    
    dfSumScore_weekly = dfScore.groupBy('Week','Patient_id').agg(sum("Session_score").alias('Sum_of_weekly_score'))
    dfSumScore_weekly = dfSumScore_weekly.sort("Week")
    
    url = 'https://https:bda-api:8100/homepage/holobalance_score'
    requests.post(url, data_to_define)
    
def main(args):
    spark = SparkSession.builder.appName(
        AppName+"_"+str(dt_string)).enableHiveSupport().getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
      hdfs_host="http://namenode"
      hdfs_port="50070"
      hdfs_path = ''.join([hdfs_host, ':', hdfs_port])
      hdfs = InsecureClient(hdfs_path)
      holobalance_score(spark, hdfs)
      
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
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


def holobalance_headache(spark, client):

    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfHeadache = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '25064002') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfHeadache = dfHeadache.distinct()
    
    dfHeadache_weekly_sum = dfHeadache.groupBy('Patient_id', 'Week', 'entry_resource_code_coding_display').agg(sum(col("entry_resource_component_valueBoolean").cast("double")).alias("Sum"))
    dfHeadache_weekly_sum = dfHeadache_weekly_sum.orderBy("Week")
    
    dfHeadache_reported = dfHeadache.withColumn("Disorientation", when(dfHeadache.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfHeadache_reported = dfHeadache_reported.select("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "Disorientation")

    
    url = 'https://https:bda-api:8100/homepage/holobalance_headache'
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
      holobalance_headache(spark, hdfs)
      
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
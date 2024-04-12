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


def holobalance_vision(spark, client):

    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfVision = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '7973008') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfVision = dfVision.distinct()
    
    dfVision_weekly_sum = dfVision.groupBy('Patient_id', 'Week', 'entry_resource_code_coding_display').agg(sum(col("entry_resource_component_valueBoolean").cast("double")).alias("Sum"))
    dfVision_weekly_sum = dfVision_weekly_sum.orderBy("Week")
    
    dfVision_reported = dfVision.withColumn("Disorientation", when(dfVision.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfVision_reported = dfVision_reported.select("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "Disorientation")

    
    url = 'https://https:bda-api:8100/homepage/holobalance_vision'
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
      holobalance_vision(spark, hdfs)
      
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
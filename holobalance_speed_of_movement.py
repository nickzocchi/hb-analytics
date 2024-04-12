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


def holobalance_speed(spark, client):

    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfSpeedOfMovement = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '282108002') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_code_text', 'entry_resource_component_valueSampledData_data', 'entry_resource_effectiveDateTime', 'Date', 'Week')
    dfSpeedOfMovement = dfSpeedOfMovement.distinct()
    dfSpeed_data = dfSpeedOfMovement.withColumn("values",split(regexp_replace("entry_resource_component_valueSampledData_data", r"(^\[\[\[)|(\]\]\]$)", ""), " "))

    speed1 = dfSpeed_data.select(dfSpeed_data.Date,dfSpeed_data.Patient_id,dfSpeed_data.Week,dfSpeed_data.entry_resource_code_coding_display,dfSpeed_data.entry_resource_component_code_text,explode(dfSpeed_data.values))
    speed2 = speed1.withColumn("col",speed1.col.cast(FloatType()))
    
    dfSpeed_weekly = speed2.groupBy("Patient_id", "Week", "entry_resource_code_coding_display").agg(mean('col').alias('Speed_of_movement'))
    dfSpeed_weekly = dfSpeed_weekly.sort('Week')

    dfSpeed_STD = speed2.groupBy("Patient_id","Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(stddev('col').alias('STD_value'))
    dfSpeed_STD = dfSpeed_STD.sort("Week")
    
    dfSpeed_min = speed2.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(min('col').alias('Min_value'))
    dfSpeed_min = dfSpeed_min.sort("Week")
    
    dfSpeed_max = speed2.groupBy("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(max('col').alias('Min_value'))
    dfSpeed_max = dfSpeed_max.sort("Week")
    
    url = 'https://https:bda-api:8100/homepage/holobalance_speed_of_movement'
    requests.post(url, data_to_define)
    
    
def segments(spark, client):
    
    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfSpeed_seconds = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '404979006') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id', 'entry_resource_code_coding_display','entry_resource_component_code_text', 'entry_resource_component_valueSampledData_data', 'entry_resource_component_valueSampledData_extension_valueString', 'entry_resource_effectiveDateTime', 'Date', 'Week')
    dfSpeed_seconds = dfSpeed_seconds.distinct()

    dfSpeed_data_seconds = dfSpeed_seconds.withColumn("data_values",split(regexp_replace("entry_resource_component_valueSampledData_data", r"(^\[\[\[)|(\]\]\]$)", ""), " "))
    dfSpeed_data_seconds = dfSpeed_data_seconds.withColumn("seconds",split(regexp_replace("entry_resource_component_valueSampledData_extension_valueString", r"(^\[\[\[)|(\]\]\]$)", ""), " "))

    arr_cols = [c[0] for c in dfSpeed_data_seconds.dtypes if c[1][:5] == "array"]
    explodedValuesSpeed = dfSpeed_data_seconds.withColumn(
        "arr_of_struct",
        arrays_zip(*[coalesce(c, array(lit(None))).alias(c) for c in arr_cols])
    ).select(
        *[c for c in dfSpeed_data_seconds.columns if c not in arr_cols],
        expr("inline(arr_of_struct)")
    )

    explodedValuesSpeed = explodedValuesSpeed.drop("entry_resource_component_valueSampledData_extension_valueString","entry_resource_component_valueSampledData_data")

    explodedValuesSpeed = explodedValuesSpeed.withColumn("data_values",col("data_values").cast(FloatType())).withColumn("seconds",col("seconds").cast(FloatType()))
    explodedValuesSpeed = explodedValuesSpeed.withColumn('sec',(explodedValuesSpeed.seconds/1000))

    df0_15_speed=explodedValuesSpeed.filter(explodedValuesSpeed.sec.between(0,15))
    dfMean_0_15_speed = df0_15_speed.groupBy("Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('0_15 Mean'))
    dfMean_0_15_speed = dfMean_0_15_speed.sort('Week')

    df15_30_speed=explodedValuesSpeed.filter(explodedValuesSpeed.sec.between(15,30))
    dfMean_15_30_speed = df15_30_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('15_30 Mean'))
    dfMean_15_30_speed = dfMean_15_30_speed.sort('Week')

    df30_45_speed=explodedValuesSpeed.filter(explodedValuesSpeed.sec.between(30,45))
    dfMean_30_45_speed = df30_45_speed.groupBy("Patient_id","Date", "Week","entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('30_45 Mean'))
    dfMean_30_45_speed = dfMean_30_45_speed.sort('Week')

    df45_60_speed=explodedValuesSpeed.filter(explodedValuesSpeed.sec.between(45,60))
    dfMean_45_60_speed = df45_60_speed.groupBy("Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('45_60 Mean'))
    dfMean_45_60_speed = dfMean_45_60_speed.sort('Week')

    dfMeanSpeed = \
    dfMean_0_15_speed.join(dfMean_15_30_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMean_30_45_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMean_45_60_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")
    

    dfSTD_0_15_speed = df0_15_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('0_15 STD'))
    dfSTD_0_15_speed = dfSTD_0_15_speed.sort('Week')

    dfSTD_15_30_speed = df15_30_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('15_30 STD'))
    dfSTD_15_30_speed = dfSTD_15_30_speed.sort('Week')
    
    dfSTD_30_45_speed = df30_45_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('30__45 STD'))
    dfSTD_30_45_speed = dfSTD_30_45_speed.sort('Week')

    dfSTD_45_60_speed = df45_60_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('45_60 STD'))
    dfSTD_45_60_speed = dfSTD_45_60_speed.sort('Week')
    
    dfSTDSpeed = \
    dfSTD_0_15_speed.join(dfSTD_15_30_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfSTD_30_45_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfSTD_45_60_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")
  
  
    dfMin_0_15_speed = df0_15_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('0_15 Min'))
    dfMin_0_15_speed = dfMin_0_15_speed.sort('Week')

    dfMin_15_30_speed = df15_30_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('15_30 Min'))
    dfMin_15_30_speed = dfMin_15_30_speed.sort('Week')

    dfMin_30_45_speed = df30_45_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('30_45 Min'))
    dfMin_30_45_speed = dfMin_30_45_speed.sort('Week')

    dfMin_45_60_speed = df45_60_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('45_60 Min'))
    dfMin_45_60_speed = dfMin_45_60_speed.sort('Week')

    dfMinSpeed = \
    dfMin_0_15_speed.join(dfMin_15_30_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMin_30_45_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMin_45_60_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")


    dfMax_0_15_speed = df0_15_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('0_15 Max'))
    dfMax_0_15_speed = dfMax_0_15_speed.sort('Week')

    dfMax_15_30_speed = df15_30_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('15_30 Max'))
    dfMax_15_30_speed = dfMax_15_30_speed.sort('Week')

    dfMax_30_45_speed= df30_45_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('30_45 Max'))
    dfMax_30_45_speed = dfMax_30_45_speed.sort('Week')

    dfMax_45_60_speed = df45_60_speed.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('45_60 Max'))
    dfMax_45_60_speed = dfMax_45_60_speed.sort('Week')

    dfMaxSpeed = \
    dfMax_0_15_speed.join(dfMax_15_30_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMax_30_45_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMax_45_60_speed, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")


    url = 'https://https:bda-api:8100/homepage/holobalance_segments_of_speed'
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
      holobalance_speed(spark, hdfs)
      segments(spark, hdfs)
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
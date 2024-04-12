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


def holobalance_sitting(spark, client):
    
    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfMovement = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '404979006') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id','entry_resource_code_coding_display', 'entry_resource_component_code_text', 'entry_resource_component_valueSampledData_data', 'entry_resource_effectiveDateTime', 'Date', 'Week')
    dfMovement = dfMovement.distinct()
    dfMovement_data = dfMovement.withColumn("values",split(regexp_replace("entry_resource_component_valueSampledData_data", r"(^\[\[\[)|(\]\]\]$)", ""), " "))
    movement1 = dfMovement_data.select(dfMovement_data.Patient_id,dfMovement_data.Date,dfMovement_data.Week,dfMovement.entry_resource_code_coding_display,dfMovement.entry_resource_component_code_text,explode(dfMovement_data.values))
    movement2=movement1.withColumn("col",movement1.col.cast(FloatType()))
    
    dfSpeedOfMovement = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '282108002') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_code_text', 'entry_resource_component_valueSampledData_data', 'entry_resource_effectiveDateTime', 'Date', 'Week')
    dfSpeedOfMovement = dfSpeedOfMovement.distinct()
    dfSpeed_data = dfSpeedOfMovement.withColumn("values",split(regexp_replace("entry_resource_component_valueSampledData_data", r"(^\[\[\[)|(\]\]\]$)", ""), " "))
    speed1 = dfSpeed_data.select(dfSpeed_data.Date,dfSpeed_data.Patient_id,dfSpeed_data.Week,dfSpeed_data.entry_resource_code_coding_display,dfSpeed_data.entry_resource_component_code_text,explode(dfSpeed_data.values))
    speed2 = speed1.withColumn("col",speed1.col.cast(FloatType()))
    
    dfRep = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '1202026006') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id','entry_resource_code_coding_display', 'entry_resource_component_valueQuantity_value', 'Date')
    dfRep = dfRep.distinct()
    dfRep = dfRep.withColumnRenamed("entry_resource_component_valueQuantity_value", "Total_reps")

    dfDuration = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '228450008') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id','entry_resource_code_coding_display', 'entry_resource_component_valueQuantity_value', 'Date','Week','Start_time')
    dfDuration = dfDuration.withColumnRenamed("entry_resource_component_valueQuantity_value", "Duration")
    dfDuration = dfDuration.distinct()

    dfSpeed_session_mean = speed2.groupBy("Patient_id","Date","entry_resource_code_coding_display", "entry_resource_component_code_text").agg(mean('col').alias('Mean_speed'))
    dfSpeed_session_mean = dfSpeed_session_mean.sort("Date")
    dfSpeed_session_max = speed2.groupBy("Patient_id","Date","entry_resource_code_coding_display", "entry_resource_component_code_text").agg(max('col').alias('Max_speed'))
    dfSpeed_session_max = dfSpeed_session_max.sort("Date")
    dfSpeed_session_min = speed2.groupBy("Patient_id","Date","entry_resource_code_coding_display", "entry_resource_component_code_text").agg(min('col').alias('Min_speed'))
    dfSpeed_session_min = dfSpeed_session_min.sort("Date")

    dfRange_mean = movement2.groupBy("Patient_id","Date", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(mean('col').alias('Mean_range'))
    dfRange_mean = dfRange_mean.sort("Date")
    dfRange_max = movement2.groupBy("Patient_id","Date", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(max('col').alias('Max_range'))
    dfRange_max = dfRange_max.sort("Date")
    dfRange_min = movement2.groupBy("Patient_id","Date", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(min('col').alias('Min_range'))
    dfRange_min = dfRange_min.sort("Date")
    
    dfDisorientation = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '62476001') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfDisorientation = dfDisorientation.distinct()
    dfDisorientation_reported = dfDisorientation.withColumn("Disorientation", when(dfDisorientation.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfDisorientation_reported = dfDisorientation_reported.select("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "Disorientation")
    
    dfVision = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '7973008') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfVision = dfVision.distinct()
    dfVision_reported = dfVision.withColumn("Abnormal Vision", when(dfVision.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfVision_reported = dfVision_reported.select("Patient_id","Date", "Week", "entry_resource_code_coding_display", "Abnormal Vision")
    
    dfDizziness = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '404640003') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfDizziness = dfDizziness.distinct()
    dfDizziness_reported = dfDizziness.withColumn("Dizziness", when(dfVision.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfDizziness_reported = dfDizziness_reported.select("Patient_id","Date", "Week","entry_resource_code_coding_display", "Dizziness")

    dfHeadache = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '25064002') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Date','Week','Patient_id', 'entry_resource_code_coding_display', 'entry_resource_component_valueBoolean')
    dfHeadache = dfHeadache.distinct()
    dfHeadache_reported = dfHeadache.withColumn("Disorientation", when(dfHeadache.entry_resource_component_valueBoolean ==True, "Yes").otherwise("No"))
    dfHeadache_reported = dfHeadache_reported.select("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "Disorientation")
    
    dfScore = dfHolo_final.filter((dfHolo_final.entry_resource_code_coding_code == '89191-1') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id','entry_resource_code_coding_display', 'entry_resource_component_valueQuantity_value', 'Date','Week','Start_time')
    dfScore = dfScore.distinct()
    dfScore = dfScore.withColumnRenamed("entry_resource_component_valueQuantity_value", "Session_score")
    

    dfSitting = \
    dfSpeed_session_min.join(dfSpeed_session_max, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfSpeed_session_mean, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfRange_min, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfRange_max, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfRange_mean, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfDisorientation_reported, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfVision_reported, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfDizziness_reported, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfHeadache_reported, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfScore, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")\
    .join(dfDuration, ["Patient_id","Date", "entry_resource_code_coding_display", "Start_time"], "full")\
    .join(dfRep, ["Patient_id","Date", "entry_resource_code_coding_display"], "full")
    dfSitting = dfSitting.drop('entry_resource_component_code_text','Week')
    dfSitting.show(truncate=False)



    url = 'https://https:bda-api:8100/homepage/holobalance_sitting'
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
      holobalance_sitting(spark, hdfs)
      
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
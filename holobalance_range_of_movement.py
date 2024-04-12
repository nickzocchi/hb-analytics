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


def holobalance_range(spark, client):

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
    
    dfMovement_weekly = movement2.groupBy("Patient_id", "Date","Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(mean('col').alias('Range_of_movement'))
    dfMovement_weekly = dfMovement_weekly.sort("Week")

    dfMovement_STD = movement2.groupBy("Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(stddev('col').alias('STD_value'))
    dfMovement_STD = dfMovement_STD.sort("Week")

    dfMovement_min = movement2.groupBy("Patient_id", "Date", "Week", "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(min('col').alias('Min_value'))
    dfMovement_min = dfMovement_min.sort("Week")

    dfMovement_max = movement2.groupBy('Patient_id','Date', 'Week', "entry_resource_code_coding_display", "entry_resource_component_code_text").agg(max('col').alias('Max_value'))
    dfMovement_max = dfMovement_max.sort("Week")

    url = 'https://https:bda-api:8100/homepage/holobalance_range_of_movement'
    requests.post(url, data_to_define)
    
def segments(spark, client):
    
    dfHolo = spark.table("holobalance")
    dfHolo = flatten_json(dfHolo)
    dfHolo_final = dfHolo_final.withColumn("Patient_id", regexp_extract('entry_resource_subject_reference', 'Patient/(.*)$', 1))
    dfHolo_final = dfHolo_final.withColumn('Date', to_date(col('entry_resource_effectiveDateTime'))).withColumn('Week', weekofyear('Date'))
    dfHolo_final = dfHolo_final.withColumn('Start_time', regexp_extract('entry_resource_effectiveDateTime', '\d\d:\d\d:\d\d\+\d\d:\d\d', 0))
    
    dfMovement_seconds = dfHolo_final.filter((dfHolo_final.entry_resource_component_code_coding_code == '404979006') & (dfHolo_final.entry_resource_code_coding_code == '1158')).select('Patient_id', 'entry_resource_code_coding_display','entry_resource_component_code_text', 'entry_resource_component_valueSampledData_data', 'entry_resource_component_valueSampledData_extension_valueString', 'entry_resource_effectiveDateTime', 'Date', 'Week')
    dfMovement_seconds = dfMovement_seconds.distinct()

    dfMovement_data_seconds = dfMovement_seconds.withColumn("data_values",split(regexp_replace("entry_resource_component_valueSampledData_data", r"(^\[\[\[)|(\]\]\]$)", ""), " "))
    dfMovement_data_seconds = dfMovement_data_seconds.withColumn("seconds",split(regexp_replace("entry_resource_component_valueSampledData_extension_valueString", r"(^\[\[\[)|(\]\]\]$)", ""), " "))

    arr_cols = [c[0] for c in dfMovement_data_seconds.dtypes if c[1][:5] == "array"]
    explodedValuesMovement = dfMovement_data_seconds.withColumn(
        "arr_of_struct",
        arrays_zip(*[coalesce(c, array(lit(None))).alias(c) for c in arr_cols])
    ).select(
        *[c for c in dfMovement_data_seconds.columns if c not in arr_cols],
        expr("inline(arr_of_struct)")
    )

    explodedValuesMovement = explodedValuesMovement.drop("entry_resource_component_valueSampledData_extension_valueString","entry_resource_component_valueSampledData_data")

    explodedValuesMovement = explodedValuesMovement.withColumn("data_values",col("data_values").cast(FloatType())).withColumn("seconds",col("seconds").cast(FloatType()))
    explodedValuesMovement = explodedValuesMovement.withColumn('sec',(explodedValuesMovement.seconds/1000))

    df0_15_movement=explodedValuesMovement.filter(explodedValuesMovement.sec.between(0,15))
    dfMean_0_15_movement = df0_15_movement.groupBy("Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('0_15 Mean'))
    dfMean_0_15_movement = dfMean_0_15_movement.sort('Week')

    df15_30_movement=explodedValuesMovement.filter(explodedValuesMovement.sec.between(15,30))
    dfMean_15_30_movement = df15_30_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('15_30 Mean'))
    dfMean_15_30_movement = dfMean_15_30_movement.sort('Week')

    df30_45_movement=explodedValuesMovement.filter(explodedValuesMovement.sec.between(30,45))
    dfMean_30_45_movement = df30_45_movement.groupBy("Patient_id","Date", "Week","entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('30_45 Mean'))
    dfMean_30_45_movement = dfMean_30_45_movement.sort('Week')

    df45_60_movement=explodedValuesMovement.filter(explodedValuesMovement.sec.between(45,60))
    dfMean_45_60_movement = df45_60_movement.groupBy("Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(mean('data_values').alias('45_60 Mean'))
    dfMean_45_60_movement = dfMean_45_60_movement.sort('Week')

    dfMeanMovement = \
    dfMean_0_15_movement.join(dfMean_15_30_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMean_30_45_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMean_45_60_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")
    

    dfSTD_0_15_movement = df0_15_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('0_15 STD'))
    dfSTD_0_15_movement = dfSTD_0_15_movement.sort('Week')

    dfSTD_15_30_movement = df15_30_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('15_30 STD'))
    dfSTD_15_30_movement = dfSTD_15_30_movement.sort('Week')
    
    dfSTD_30_45_movement = df30_45_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('30__45 STD'))
    dfSTD_30_45_movement = dfSTD_30_45_movement.sort('Week')

    dfSTD_45_60_movement = df45_60_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(stddev('data_values').alias('45_60 STD'))
    dfSTD_45_60_movement = dfSTD_45_60_movement.sort('Week')
    
    dfSTDMovement = \
    dfSTD_0_15_movement.join(dfSTD_15_30_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfSTD_30_45_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfSTD_45_60_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")
  
  
    dfMin_0_15_movement = df0_15_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('0_15 Min'))
    dfMin_0_15_movement = dfMin_0_15_movement.sort('Week')

    dfMin_15_30_movement = df15_30_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('15_30 Min'))
    dfMin_15_30_movement = dfMin_15_30_movement.sort('Week')

    dfMin_30_45_movement = df30_45_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('30_45 Min'))
    dfMin_30_45_movement = dfMin_30_45_movement.sort('Week')

    dfMin_45_60_movement = df45_60_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(min('data_values').alias('45_60 Min'))
    dfMin_45_60_movement = dfMin_45_60_movement.sort('Week')

    dfMinMovement = \
    dfMin_0_15_movement.join(dfMin_15_30_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMin_30_45_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMin_45_60_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")


    dfMax_0_15_movement = df0_15_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('0_15 Max'))
    dfMax_0_15_movement = dfMax_0_15_movement.sort('Week')

    dfMax_15_30_movement = df15_30_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('15_30 Max'))
    dfMax_15_30_movement = dfMax_15_30_movement.sort('Week')

    dfMax_30_45_movement= df30_45_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('30_45 Max'))
    dfMax_30_45_movement = dfMax_30_45_movement.sort('Week')

    dfMax_45_60_movement = df45_60_movement.groupBy("Patient_id","Date","Week", "entry_resource_code_coding_display","entry_resource_component_code_text").agg(max('data_values').alias('45_60 Max'))
    dfMax_45_60_movement = dfMax_45_60_movement.sort('Week')

    dfMaxMovement = \
    dfMax_0_15_movement.join(dfMax_15_30_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMax_30_45_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")\
    .join(dfMax_45_60_movement, ["Patient_id","Date", "Week", "entry_resource_code_coding_display","entry_resource_component_code_text"], "full")


    url = 'https://https:bda-api:8100/homepage/holobalance_segments_of_movement'
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
      holobalance_range(spark, hdfs)
      segments(spark, hdfs)
      
     
    except:
      raise

    spark.stop()
    return None

if __name__ == "__main__":
    main(sys.argv)
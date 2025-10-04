import dlt
from pyspark.sql.functions import *;
from pyspark.sql.types import *;


@dlt.view(

    name="stg_flights"
)

def stg_flights():
    data=spark.readStream.format('delta').load("/Volumes/workspace/bronze_schema/bronze/flights/data/");
    data=data.withColumn('flight_date',to_date( col('flight_date') ) ) \
    .drop("_rescued_data"); 
    return data;


dlt.create_streaming_table(
    name="silver_layer_flights"
)

dlt.create_auto_cdc_flow(
    target="silver_layer_flights",
    source="stg_flights",
    keys=["flight_id"],
    sequence_by=col("flight_id"),
    stored_as_scd_type=1
)


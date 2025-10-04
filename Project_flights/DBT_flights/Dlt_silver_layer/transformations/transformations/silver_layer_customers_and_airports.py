import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.view(
    name="stg_customers",
)
def stg_customers():
    data = spark.readStream.format("delta").load("/Volumes/workspace/bronze_schema/bronze/customers/data")
    data = data.withColumn("modified_date", current_timestamp())
    data = data.drop(col('_rescued_data'))
    return data

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
    target='silver_customers',
    source='stg_customers',
    keys=['passenger_id'],
    sequence_by=col("passenger_id"),
    stored_as_scd_type="1"
)

@dlt.view(
    name="stg_airports",
)
def stg_airports():
    data = spark.readStream.format("delta").load("/Volumes/workspace/bronze_schema/bronze/airports/data")
    data = data.withColumn("modified_date", current_timestamp())
    data = data.drop(col('_rescued_data'))
    return data

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target='silver_airports',
    source='stg_airports',
    keys=['airport_id'],
    sequence_by=col('airport_id'),
    stored_as_scd_type="1"
)

@dlt.table(
    name='silver_bussiness'
)
def silver_bussiness():
    data = (
        spark.readStream.table('silver_bookings')
        .join(spark.readStream.table('silver_customers'), ['passenger_id'])
        .join(spark.readStream.table('silver_airports'), ['airport_id'])
        .join(spark.readStream.table('silver_layer_flights'), ['flight_id'])
        .select(
            'airport_id',
            'passenger_id',
            'booking_id',
            'flight_id',
            'booking_date',
            'amount',           # from silver_bookings
            'airline',          # from silver_layer_flights
            'airport_name',     # from silver_airports
            'city',             # from silver_airports
            'country',          # from silver_airports
            'name',             # from silver_customers
            'gender',           # from silver_customers
            'nationality'       # from silver_customers
        )
    )
    return data
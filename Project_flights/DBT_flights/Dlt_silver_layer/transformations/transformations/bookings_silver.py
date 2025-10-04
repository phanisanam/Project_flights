import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name="stg_bookings",
)
def stg_bookings():
    data=spark.readStream.format("delta").load("/Volumes/workspace/bronze_schema/bronze/bookings/data");
    return data;

@dlt.view(
    name="trans_booking"
)

def trans_booking():
    data=spark.readStream.table('stg_bookings')
    data=data.withColumn("amount",col('amount').cast(DoubleType()) ) \
    .withColumn("booking_date",to_date( col('booking_date') ) ) \
        .withColumn("modeified_date",current_timestamp() ) \
            .drop(col('_rescued_data'))
    return data;
    
expectations={"rule1":"amount>0",
              "rule2":"booking_id is not null"
              }

@dlt.table(
    name="silver_bookings"
)

@dlt.expect_all_or_drop(expectations)
def silver_bookings():
    data=spark.readStream.table('trans_booking');
    return data;

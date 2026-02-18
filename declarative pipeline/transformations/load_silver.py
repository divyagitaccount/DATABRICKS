# from pyspark import pipelines as dp
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

def word_to_num(value):
     if value is None:
        return None
     try:
        return int(value)
     except:
        try:
            return w2n.word_to_num(str(value).lower())
        except:
            return None
convert_age_udf = udf(word_to_num)

@dp.materialized_view(
    name="silver_staff_dlt",
    comment="standardised staff data"
)
def silver_staff_dlt():
    return (
            spark.read.table("bronze_staff_data")
            .select(
            col("_c0").cast(LongType()).alias("shipment_id"),
            col("_c1").cast(StringType()).alias("first_name"),
            col("_c2").cast(StringType()).alias("last_name"),
            col("_c3").cast(IntegerType()).alias("age"),
            col("_c4").cast(StringType()).alias("role"),
            col("_c5").cast(StringType()).alias("hub_location"),
            col("_c6").cast(StringType()).alias("vehicle_type")
        )
        .select(
            col("shipment_id").cast("bigint"),
            convert_age_udf(col("age")).alias("age"),
            lower(col("role")).alias("role"),
            initcap(col("hub_location")).alias("origin_hub_city"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("staff_full_name"),
            initcap(col("vehicle_type")).alias("vehicle_type")
        )

    )

@dp.materialized_view(
    name="silver_geotag_dlt",
    comment="Cleaned geotag data",
    table_properties={"quality": "silver"}
)
def silver_geotag_dlt2():
    return (
        spark.read.table("bronze_geotag_data")
        .select(
            col("_c0").cast(StringType()).alias("city_name"),
            col("_c1").cast(StringType()).alias("country"),
            col("_c2").cast(DoubleType()).alias("latitude"),
            col("_c3").cast(DoubleType()).alias("longitude")
        )
        .select(
            initcap(col("city_name")).alias("city_name"),
            initcap(col("country")).alias("masked_hub_location"),
            col("latitude"),
            col("longitude")
        )
)

@dp.materialized_view(
    name="silver_shipments_dlt",
    comment="Enriched and split shipments data",
    table_properties={"quality": "silver"}
)
def silver_shipments_dlt():
    ship_date_col = to_date(col("shipment_date"), "yy-MM-dd")
    
    return (
        spark.read.table("bronze_shipments_data")
        .withColumn("domain", lit("Logistics"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("is_expedited_flag_initial", lit(False).cast("boolean"))
        .withColumn("shipment_date_clean", ship_date_col)
        .withColumn("shipment_cost_clean", round(col("shipment_cost"), 2))
        .withColumn("shipment_weight_clean", col("shipment_weight_kg").cast("double"))
        .withColumn("route_segment", concat_ws("-", col("source_city"), col("destination_city")))
        .withColumn("vehicle_identifier", concat_ws("_", col("vehicle_type"), col("shipment_id")))
        .withColumn("shipment_year", year(ship_date_col))
        .withColumn("shipment_month", month(ship_date_col))
        .withColumn("is_weekend", 
            when(dayofweek(ship_date_col).isin([1, 7]), True)
            .otherwise(False)
        )
        .withColumn("is_expedited", 
            when(col("shipment_status").isin(["IN_TRANSIT", "DELIVERED"]), True)
            .otherwise(False)
        )
        .withColumn("cost_per_kg", round(col("shipment_cost") / col("shipment_weight_kg"), 2))
        .withColumn("tax_amount", round(col("shipment_cost") * 0.18, 2))
        .withColumn("days_since_shipment", datediff(current_date(), ship_date_col))
        .withColumn("is_high_value", 
            when(col("shipment_cost") > 50000, True)
            .otherwise(False))
        .withColumn("order_prefix", substring(col("order_id"), 1, 3))
        .withColumn("order_sequence", substring(col("order_id"), 4, 10))
        .withColumn("ship_day", dayofmonth(ship_date_col))
        .withColumn("route_lane", concat_ws("->", col("source_city"), col("destination_city")))
    )
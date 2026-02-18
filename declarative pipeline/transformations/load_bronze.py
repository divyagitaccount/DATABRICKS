from pyspark import pipelines as dp

@dp.table(name="bronze_staff_data")
def bronze_staff():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","false")
            #.option("inferColumnTypes","false") #cloudFiles.inferColumnTypes to true causes Spark to automatically assume the file contains headers.
            # both do type detecetion in differnt context in spark
            #inferSchema → Normal Spark file reading
            #inferColumnTypes → Auto Loader / streaming ingestion
            #Mostly streaming ingestion ,inferschema is usd in batch ingestion
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/bronze/staff1/")
            )

@dp.table(name="bronze_geotag_data")
def bronze_geotag():
     return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("header","false")
            .option("inferColumnTypes","false") 
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/bronze/geotag1/")
            )
@dp.table(name="bronze_shipments_data")
def bronze_shipments():
     
     return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","json")
            .option("header","false")
            .option("inferColumnTypes","true") 
            .option("multiline","true")
            .load("/Volumes/prodcatalog_wd36/logistics_wd36/bronze/shipments1/")
          
            )

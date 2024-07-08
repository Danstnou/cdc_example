package com.example

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path

object Main extends App {
  val spark: SparkSession = {
    val sparkSession =
      SparkSession.builder()
        .master("local[*]")
        .enableHiveSupport()
        .appName("CDCWithDeltaLake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", false)
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  // для решения проблемы с путями в windows
  private val parentPath = Path.of("").toAbsolutePath.toString.replace("\\", "/")
  private val path = parentPath + "/spark-warehouse/product"
  private val checkpointLocation = parentPath + "/spark-warehouse/checkpoints"

  private def getDeltaTable = DeltaTable.forPath(spark, path)

  private val schemaFromDebezium = StructType(Seq(StructField("payload", StringType)))
  private val messageSchema =
    StructType(Seq(
      StructField("before", MapType(StringType, StringType)),
      StructField("after", MapType(StringType, StringType)),
      StructField("op", StringType)
    ))

  private def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
    DeltaTable.createIfNotExists(spark).location(path).addColumns(microBatchOutputDF.schema).execute

    getDeltaTable.as("t")
      .merge(microBatchOutputDF.as("s"), "s.id = t.id")
      .whenMatched("s.op = 'd'").delete()
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()

    getDeltaTable.toDF.show(false)
  }

  spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "cdc.public.product")
    .option("startingOffsets", "earliest")
    .option("multiline", "true")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schemaFromDebezium).alias("data"))
    .selectExpr("data.payload")
    .select(from_json(col("payload"), messageSchema).alias("data"))
    .selectExpr("case when data.after is null then data.before.id else data.after.id end as id",
      "data.after.name", "data.after.price", "data.op")
    .filter("id is not null")
    .writeStream
    .format("delta")
    .foreachBatch(upsertToDelta _)
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation)
    .start()
    .awaitTermination
}
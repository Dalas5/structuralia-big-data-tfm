package com.dalas

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, DateType}
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.SparkSession
import org.apache.hudi.keygen.ComplexKeyGenerator
import java.sql.Date
import java.io.File


object DataLakeValidation {

  case class Review(marketplace: String, customer_id: Int, review_id: String, product_id: String,
                    product_parent: Int, product_title: String, product_category: String,
                    star_rating: String, helpful_votes: Int, total_votes: Int, vine: String,
                    verified_purchase: String, review_headline: String, review_body: String, review_date: Date)

  def main(args: Array[String]): Unit = {

    // Start time
    val t1 = System.nanoTime()

    if (args.length > 5) {
      println("Need input path, table name, output path, start date and end date")
      System.exit(1)
    }

    // Default values definition
    val defaultArchivePath = "/home/dalas/Downloads/archive"
    val defaultTableName = "amazon_reviews"
    val defaultProjectArchiveBasePath = "s3a://amazon-data"
    val defaultStartdt = "1998-07-15"
    val defaultEnddt = "1999-12-31"


    // Parse command line arguments
    val inputPath = if (args.length > 0) args(0) else defaultArchivePath
    val outputPath = if (args.length > 1) args(1) else defaultProjectArchiveBasePath
    val tableName = if (args.length > 2) args(2) else defaultTableName
    val startdt = if (args.length > 3) args(3) else defaultStartdt
    val enddt = if (args.length == 5) args(4) else defaultEnddt

    println(s"Job parameters are: \ninputPath: $inputPath\noutputPath: $outputPath\ntableName: $tableName\n" +
      s"startdt: $startdt    enddt: $enddt")

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Data Lake Ingestion Validation")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.access.key", "dalas")
      .config("spark.hadoop.fs.s3a.secret.key", "devdeav5")
      .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.14:9000")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType")
      .getOrCreate()

    // Set the log level to only print errors
    spark.sparkContext.setLogLevel("ERROR")

    // UDF to convert string star_rating to int safely
    val udfInt = udf((s: String) => if (s.forall(Character.isDigit)) s.toInt else -1)

    val reviewSchema = new StructType()
      .add("marketplace", StringType, nullable = true)
      .add("customer_id", IntegerType, nullable = true)
      .add("review_id", StringType, nullable = true)
      .add("product_id", StringType, nullable = true)
      .add("product_parent", IntegerType, nullable = true)
      .add("product_title", StringType, nullable = true)
      .add("product_category", StringType, nullable = true)
      .add("star_rating", StringType, nullable = true)
      .add("helpful_votes", IntegerType, nullable = true)
      .add("total_votes", IntegerType, nullable = true)
      .add("vine", StringType, nullable = true)
      .add("verified_purchase", StringType, nullable = true)
      .add("review_headline", StringType, nullable = true)
      .add("review_body", StringType, nullable = true)
      .add("review_date", DateType, nullable = true)


    import spark.implicits._
    val amazonDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(reviewSchema)
      .csv(inputPath)
      .as[Review]

    val intRatingAmazonDF = amazonDF
      .withColumn("int_star_rating",
        when(col("star_rating").isNotNull, udfInt(col("star_rating"))).otherwise(lit(null)))
      .filter(col("review_id").isNotNull)
      .where(col("review_date").between(startdt, enddt))

    intRatingAmazonDF.createOrReplaceTempView("amazon_reviews")

    spark.sql(
      """select
        |count(*) as orig_row_count,
        |sum(star_rating) as orig_star_rating,
        |sum(helpful_votes) as orig_helpful_votes,
        |sum(total_votes) as orig_total_votes
        |from amazon_reviews""".stripMargin).show()

    intRatingAmazonDF.show(5, 20, vertical = false)


    val preAmazonHudiDF = spark.
    read.
    format("hudi").
    load(outputPath + File.separator + tableName)

    val amazonHudiDF = preAmazonHudiDF.where(col("review_date").between(startdt,enddt))

    amazonHudiDF.createOrReplaceTempView("hudi_amazon_reviews")
    spark.sql(
      """select
        |count(*) as hudi_row_count,
        |sum(star_rating) as hudi_star_rating,
        |sum(helpful_votes) as hudi_helpful_votes,
        |sum(total_votes) as hudi_total_votes
        |from hudi_amazon_reviews""".stripMargin).show()

    amazonHudiDF.show(5, 20, vertical = false)


    spark.stop()

    // Printing runtime duration in minutes
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Run time: $duration")

  }

}

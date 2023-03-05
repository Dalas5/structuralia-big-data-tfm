package com.dalas

import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructType}
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.SparkSession
import org.apache.hudi.keygen.ComplexKeyGenerator

import java.sql.Date
import java.io.File
import scala.reflect.ClassTag
import scala.util.Random


object AmazonEDA {

  def main(args: Array[String]): Unit = {

    // Start time
    val t1 = System.nanoTime()

    if (args.length > 5) {
      println("Need input path, table name, output path, start date and end date")
      System.exit(1)
    }

    // Default values definition
    val defaultProjectArchiveBasePath = "s3a://amazon-data"
    val defaultEDAProjectPath = "s3a://amazon-eda"
    val defaultTableName = "amazon_reviews"
    val defaultStartdt = "1998-07-15"
    val defaultEnddt = "1999-12-31"

    // Parse command line arguments
    val inputPath = if (args.length > 0) args(0) else defaultProjectArchiveBasePath
    val outputPath = if (args.length > 1) args(1) else defaultEDAProjectPath
    val tableName = if (args.length > 2) args(2) else defaultTableName
    val startdt = if (args.length > 3) args(3) else defaultStartdt
    val enddt = if (args.length == 5) args(4) else defaultEnddt

    println(s"Job parameters are: \ninputPath: $inputPath\noutputPath: $outputPath\ntableName: $tableName\n" +
      s"startdt: $startdt    enddt: $enddt     source_table_path: ${inputPath + File.separator + tableName}")


    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Data Lake Exploratory Data Analysis")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.hadoop.fs.s3a.access.key", "dalas")
      .config("spark.hadoop.fs.s3a.secret.key", "devdeav5")
      .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.14:9000")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType")
      .master("local[*]")
      .getOrCreate()

    // Set the log level to only print errors
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val preAmazonHudiDF = spark.
      read.
      format("hudi").
      load(inputPath + File.separator + tableName)

    val amazonHudiDF = preAmazonHudiDF
      .select("marketplace", "customer_id", "review_id", "product_id", "product_parent",
          "product_title", "product_category", "star_rating", "helpful_votes", "total_votes",
          "vine", "verified_purchase", "review_headline", "review_body", "review_date", "int_star_rating")
      .where(col("review_date").between(startdt, enddt))

    amazonHudiDF.createOrReplaceTempView("hudi_amazon_reviews")

    val initialRowCount = amazonHudiDF.count()
    println(s"Count of initial DF: $initialRowCount")

    /* EDA Questions */

    // Q1: Counting nulls in relevant columns

    // Custom function that counts null values for each column_name in array
    def countCols(columns: Array[String]) = {
      columns.map(
        c => count(when(col(c).isNull, c)).alias(c)
      )
    }

    // Creating DataFrame with all same columns and their null counts (contains just one row)
    val nullCountDF = amazonHudiDF.select(countCols(amazonHudiDF.columns): _*)
    nullCountDF.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator + "null-count")


    // Cleaning: Dropping nulls in relevant columns
    val cleanAmazonDF = amazonHudiDF
      .na.drop(Seq(
      "customer_id",
      "review_id",
      "product_id",
      "product_parent",
      "product_title",
      "product_category",
      "star_rating",
      "review_body",
      "review_date"))
      .withColumn("word_count", size(split(col("review_body"), "\\s+")))
      .withColumn("year", year(col("review_date")))

    val cleanCount = cleanAmazonDF.count()
    println(s"Count post data cleansing: $cleanCount")
    println(s"Dropped off data: ${initialRowCount - cleanCount}")

    cleanAmazonDF.repartition(96)


    // Q2 Star Rating distribution

    val starRatingDist = cleanAmazonDF.select("product_id", "star_rating", "review_date")

    starRatingDist.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator +"star-rating-dist")


    // Q3: Calculate and save average rating per product
    val avgProductRating = cleanAmazonDF
      .groupBy("product_category", "product_id")
      .agg(avg("star_rating").as("avg_stars"))
      .orderBy(col("avg_stars").desc_nulls_last)

    avgProductRating.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator + "avg-product-rating")


    // Q4: Generate basic statistics for relevant value columns
    val valueColDescription = cleanAmazonDF.select("star_rating", "helpful_votes", "total_votes").describe()
    valueColDescription.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator + "describe")


    // Q5: Generate word count distribution
    val wordCountDF = cleanAmazonDF.groupBy("word_count").agg(count("*").as("review_count"))
    wordCountDF.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator + "word-count")


    /* BUSINESS QUESTIONS */

    // Q6: How to detect products that should be dropped or kept available
    val prodIdList = cleanAmazonDF
      .select("product_id")
      .distinct()
      .map(f => f.getString(0))
      .collect().toList

    def takeSample[T: ClassTag](a: Array[T], n: Int, seed: Long) = {
      val rnd = new Random(seed)
      Array.fill(n)(a(rnd.nextInt(a.length)))
    }

    val prodIdSample = takeSample(prodIdList.toArray, 30, 42)

    val starRatingDistSampled = starRatingDist.filter(col("product_id").isin(prodIdSample: _*))
    starRatingDistSampled.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator +"product-performance")




    // Q7: Evolution by year and by product_category of the qty of reviews
    val reviewQtyEvolution = cleanAmazonDF
      .groupBy("year", "product_category")
      .agg(count("*").as("review_count"))

    reviewQtyEvolution.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator +"review-qty-evolution")


    // Q8: Is there a biased between verified-purchase reviews and those that are not?
    val verifiedBias = cleanAmazonDF
      .groupBy("verified_purchase", "product_category")
      .agg(avg("star_rating").as("avg_rating"))

    verifiedBias.write
      .option("header", "true")
      .mode(Overwrite)
      .csv(outputPath + File.separator +"verified-bias")

    spark.stop()

    // Printing runtime duration in minutes
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Run time: $duration")

  }

}

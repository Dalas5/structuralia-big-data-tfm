package com.dalas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{DenseVector, VectorUDT}
import org.apache.spark.ml.feature.{CountVectorizer, IDF, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

import java.sql.Date
import java.io.File
object AmazonCFR {

  def main(args: Array[String]): Unit = {

    // Start time
    val t1 = System.nanoTime()


    if (args.length > 5) {
      println("Need input path, table name, output path, start date and end date")
      System.exit(1)
    }

    // Default values definition
    val defaultProjectArchiveBasePath = "s3a://amazon-data"
    val defaultNLPProjectPath = "s3a://amazon-ml/amazon-cfr"
    val defaultTableName = "amazon_reviews"
    val defaultStartdt = "2015-01-01"
    val defaultEnddt = "2015-01-31"

    // Parse command line arguments
    val inputPath = if (args.length > 0) args(0) else defaultProjectArchiveBasePath
    val outputPath = if (args.length > 1) args(1) else defaultNLPProjectPath
    val tableName = if (args.length > 2) args(2) else defaultTableName
    val startdt = if (args.length > 3) args(3) else defaultStartdt
    val enddt = if (args.length == 5) args(4) else defaultEnddt

    println(s"Job parameters are: \ninputPath: $inputPath\noutputPath: $outputPath\ntableName: $tableName\n" +
      s"startdt: $startdt    enddt: $enddt     source_table_path: ${inputPath + File.separator + tableName}")


    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder()
      .appName("Amazon Collaborative Filtering Recommendation Analysis")
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


    // Load each file of the source data into an Dataset
    val pre_amazonDF = spark.
      read.
      format("hudi").
      load(inputPath + File.separator + tableName)

    val amazonDF = pre_amazonDF.select("marketplace", "customer_id", "review_id", "product_id", "product_parent",
      "product_title", "product_category", "star_rating", "helpful_votes", "total_votes",
      "vine", "verified_purchase", "review_headline", "review_body", "review_date", "int_star_rating")
      .where(col("review_date").between(startdt, enddt))
      .where(col("product_category") === "PC")


    // Dropping rows containing nulls in "customer_id", "product_id", "star_rating"
    val df = amazonDF.select("customer_id", "product_id", "product_title",  "int_star_rating")
      .na.drop(Seq("customer_id", "product_id",  "product_title", "int_star_rating"))
      .withColumn("int_customer_id", dense_rank().over(Window.orderBy("customer_id")))
      .withColumn("int_product_id", dense_rank().over(Window.orderBy("product_id")))

    // Creating product look up table

    val prd_lkp = df.select("int_product_id", "product_title").dropDuplicates()

    // split data set training(70%) and test(30%)
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    // build recommendation model with als algorithm
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("int_customer_id")
      .setItemCol("int_product_id")
      .setRatingCol("int_star_rating")
    val alsModel = als.fit(trainingData)


    // evaluate the als model
    // compute root mean square error(rmse) with test data for evaluation
    // set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    alsModel.setColdStartStrategy("drop")
    val predictions = alsModel.transform(testData)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("int_star_rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"root mean square error $rmse")

    // top 10 book recommendations for each user
    val allUserRec = alsModel.recommendForAllUsers(10)
    allUserRec.printSchema()
    allUserRec.show(10, 0, true)

    import spark.implicits._
    // Unnesting structure and adding item description
    val allUserRecPreDesc = allUserRec
      .select($"int_customer_id", explode($"recommendations").as("recommendation"))

    allUserRecPreDesc.printSchema()

    val allUsersRecDesc =allUserRecPreDesc
    .join(prd_lkp, allUserRecPreDesc("recommendation.int_product_id") === prd_lkp("int_product_id"), "left")
    .orderBy($"int_customer_id")

    allUsersRecDesc.show(30, 0, true)


    // top 10 book recommendations for specific set of users(3 users)
    val userRec = alsModel.recommendForUserSubset(df.select("int_customer_id").distinct().limit(3), 10)
    userRec.printSchema()
    userRec.show(10, 0, true)


    // Printing runtime duration in minutes
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Run time: $duration")

  }

}

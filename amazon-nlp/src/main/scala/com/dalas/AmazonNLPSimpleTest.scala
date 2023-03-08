package com.dalas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{DenseVector, VectorUDT}
import org.apache.spark.ml.feature.{CountVectorizer, IDF, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, VectorUDT}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.keygen.ComplexKeyGenerator
import java.sql.Date
import java.io.File
object AmazonNLPSimpleTest {

  def main(args: Array[String]): Unit = {
    // Start time
    val t1 = System.nanoTime()

    if (args.length > 3) {
      println("Need input path, model name and pipeline name")
      System.exit(1)
    }

    // Default values definition
    val defaultNLPProjectPath = "s3a://amazon-ml/amazon-nlp"
    val defaultModelName = "nlp-model.3.12"
    val defaultPipelineName = "nlp_pipeline.3.12"


    // Parse command line arguments
    val inputPath = if (args.length > 0) args(0) else defaultNLPProjectPath
    val modelName = if (args.length > 1) args(1) else defaultModelName
    val pipelineName = if (args.length > 2) args(2) else defaultPipelineName


    println(s"Job parameters are: \ninputPath: $inputPath\nmodel name: $modelName\n" +
      s"nlp pipeline name: $pipelineName")


    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder()
      .appName("Amazon Sentiment Analysis")
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

    def avg_wordvecs_fun = (wordvecs: Seq[Seq[Double]]) => {
      val length = wordvecs.length
      val maxLength = wordvecs.maxBy(_.length).length
      val zeroSeq = Seq.fill[Double](maxLength)(0.0)
      val sumSeq = wordvecs.foldLeft(zeroSeq)((a, x) => (a zip x).map { case (u, v) => u + v })
      val averageSeq = sumSeq.map(_ / length.toDouble)
      new DenseVector(averageSeq.toArray)
    }

    val avg_wordvecs = spark.udf.register("avg_wordvecs", avg_wordvecs_fun)

    val nlp_pipeline = PipelineModel.load(inputPath + File.separator + pipelineName)
    val model = PipelineModel.load(inputPath + File.separator + modelName)

    import spark.implicits._

    val data = Seq("I'm totally disappointed, It broke the next day I used",
      "I loved this product, it's great quality",
      "It is ok for the price, but I don't recommend it",
      "I hoped for more, but is acceptable quality, three stars")
      .toDF("review_body")

    val nlp_procd = nlp_pipeline.transform(data)

    val dataTransformed = nlp_procd.selectExpr(
      "review_body",
      "normalized.result AS normalized",
      "embeddings.embeddings")

    val demoWithAvg = dataTransformed
      .withColumn("avg_wordvec", avg_wordvecs(col("embeddings")))
      .drop("embeddings")

    val preds = model.transform(demoWithAvg)

    preds.select("review_body", "prediction").show(false)

  }

}

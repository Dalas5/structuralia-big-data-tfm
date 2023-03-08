package com.dalas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
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
import java.sql.Date
import java.io.File

object AmazonNLP {

  def main(args: Array[String]): Unit = {

    // Start time
    val t1 = System.nanoTime()

    if (args.length > 5) {
      println("Need input path, table name, output path, start date and end date")
      System.exit(1)
    }

    // Default values definition
    val defaultProjectArchiveBasePath = "s3a://amazon-data"
    val defaultNLPProjectPath = "s3a://amazon-ml/amazon-nlp"
    val defaultTableName = "amazon_reviews"
    val defaultStartdt = "1998-07-15"
    val defaultEnddt = "1999-12-31"

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

    // Load each file of the source data into an Dataset
    val pre_amazonDF = spark.
      read.
      format("hudi").
      load(inputPath + File.separator + tableName)

    val amazonDF = pre_amazonDF.select("marketplace", "customer_id", "review_id", "product_id", "product_parent",
      "product_title", "product_category", "star_rating", "helpful_votes", "total_votes",
      "vine", "verified_purchase", "review_headline", "review_body", "review_date", "int_star_rating")
      .where(col("review_date").between(startdt, enddt))

    // UDF to re-scale star_rating from reviews to: 0 - Negative, 1 - Neutral, 2 - Positive
    val udfSentiment = udf((rating: String) => {
      rating match {
        case "1" | "2" => 0
        case "3" => 1
        case "4" | "5" => 2
      }
    })


    // Save initial row count
    val initialRowCount = amazonDF.count()
    println(s"Count of initial DF: $initialRowCount")

    // Dropping rows containing nulls in "review_id", "star_rating", "review_body"
    val df = amazonDF.select("review_id", "star_rating", "review_body")
      .na.drop(Seq("review_id", "star_rating", "review_body"))
      .withColumn("label", udfSentiment(col("star_rating")))

    // filtering by word count (at least 3 words)
    val dfWithWordCount = df.withColumn("word_count", size(split(col("review_body"), "\\s+")))
    val rowsLessThanThreeWords = dfWithWordCount.filter(col("word_count").between(3,2000)).count()
    println(s"Reviews with less than 3 words and no more than 2000: $rowsLessThanThreeWords")

    val dfCleaned = dfWithWordCount.filter(col("word_count").between(3,2000))


    // Divide training and testing data sets
    val test_df = dfCleaned.stat.sampleBy(
      col("star_rating"),
      Map("1" -> 0.2, "2" -> 0.2, "3" -> 0.2, "4" -> 0.2, "5" -> 0.2),
      42)

    test_df.persist()

    val train_df = dfCleaned.join(
      test_df,
      df.col("review_id") === test_df.col("review_id"),
      "left_anti")

    train_df.persist()

    println("test and train df counts:")

    val testCount = test_df.count() // 4,416,380
    val trainCount = train_df.count() // 17,664,889
    println(s"Test count: $testCount | Train count: $trainCount")

    test_df.groupBy("star_rating")
      .agg(count("star_rating") / testCount)
      .orderBy(col("star_rating").desc_nulls_last)
      .show()

    train_df.groupBy("star_rating")
      .agg(count("star_rating") / trainCount)
      .orderBy(col("star_rating").desc_nulls_last)
      .show()


    // Creating NLP pipeline
    val assembler = new DocumentAssembler()
      .setInputCol("review_body")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols(Array("document"))
      .setOutputCol("sentences")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("sentences"))
      .setOutputCol("tokens")

    val lemmatizer = LemmatizerModel.pretrained()
      .setInputCols(Array("tokens"))
      .setOutputCol("lemmas")

    val normalizer = new Normalizer()
      .setCleanupPatterns(Array(
        "[^a-zA-Z.-]+",
        "^[^a-zA-Z]+",
        "[^a-zA-Z]+$"))
      .setInputCols(Array("lemmas"))
      .setOutputCol("normalized")
      .setLowercase(true)

    val glove = WordEmbeddingsModel.pretrained(name = "glove_100d")
      .setInputCols(Array("document", "normalized"))
      .setOutputCol("embeddings")

    val nlp_pipeline = new Pipeline().setStages(Array(
      assembler, sentence, tokenizer, lemmatizer, normalizer, glove
    )).fit(train_df)

    // Selecting original data and normalized tokens and embeddings
    val train_transformed_df = nlp_pipeline.transform(train_df)
      .selectExpr(
        "review_id", "review_body", "label",
        "normalized.result AS normalized",
        "embeddings.embeddings")

    val test_transformed_df = nlp_pipeline.transform(test_df)
      .selectExpr(
        "review_id", "review_body", "label",
        "normalized.result AS normalized",
        "embeddings.embeddings"
      )

    nlp_pipeline.write.overwrite.save(outputPath + File.separator + "nlp_pipeline.3.12")

    // Defining doc2vec: average the word vectors in a document vector

    def avg_wordvecs_fun = (wordvecs: Seq[Seq[Double]]) => {
      val length = wordvecs.length
      val maxLength = wordvecs.maxBy(_.length).length
      val zeroSeq = Seq.fill[Double](maxLength)(0.0)
      val sumSeq = wordvecs.foldLeft(zeroSeq)((a, x) => (a zip x).map { case (u, v) => u + v })
      val averageSeq = sumSeq.map(_ / length.toDouble)
      new DenseVector(averageSeq.toArray)
    }

    val avg_wordvecs = spark.udf.register("avg_wordvecs", avg_wordvecs_fun)

    // Debugging avg_wordvecs_failure
    val train_transformed_count = train_transformed_df.count()
    println(s"Train record count before filtering embeddings: $train_transformed_count")

    // Filtering train data from empty embeddings (causes errors)
    val train_transformed_df_cleaned = train_transformed_df
      .withColumn("size_embeddings", size(col("embeddings")))
      .filter(col("size_embeddings") >= 1)
      .drop("size_embeddings")

    val zeroEmbeddingsTrain = train_transformed_df_cleaned.count()
    println(s"Train records count after filtering embeddings: $zeroEmbeddingsTrain")


    // Filtering test data from empty embeddings (causes errors)
    val test_transformed_count = test_transformed_df.count()
    println(s"Test record count before filtering embeddings: $test_transformed_count")

    val test_transformed_df_cleaned = test_transformed_df
      .withColumn("size_embeddings", size(col("embeddings")))
      .filter(col("size_embeddings") >= 1)
      .drop("size_embeddings")

    val zeroEmbeddingsTest = test_transformed_df_cleaned.count()
    println(s"Test records after filtering embeddings: $zeroEmbeddingsTest")


    val trainedWithAvg = train_transformed_df_cleaned
      .withColumn("avg_wordvec", avg_wordvecs(col("embeddings")))
      .drop("embeddings")

    val testedtWithAvg = test_transformed_df_cleaned
      .withColumn("avg_wordvec", avg_wordvecs(col("embeddings")))
      .drop("embeddings")

    // trainedWithAvg.write.mode("overwrite").parquet("amazon.train")
    // testedWithAvg.write.mode("overwrite").parquet("amazon.test")

    // Cleaning up data that was persisted before
    test_df.unpersist()
    train_df.unpersist()

    // Featurize
    val tf = new CountVectorizer()
      .setInputCol("normalized")
      .setOutputCol("tf")

    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tfidf")

    val featurizer = new Pipeline().setStages(Array(tf, idf))

    // Model
    val vec_assembler = new VectorAssembler()
      .setInputCols(Array("avg_wordvec"))
      .setOutputCol("features")

    val logreg = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val model_pipeline = new Pipeline()
      .setStages(Array(featurizer, vec_assembler, logreg))

    val model = model_pipeline.fit(trainedWithAvg)

    // Saving the model
    model.write.overwrite.save(outputPath + File.separator + "nlp-model.3.12")

    // Evaluate

    val train_preds = model.transform(trainedWithAvg)
    val test_preds = model.transform(testedtWithAvg)

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("f1")

    val resultTrainPreds = evaluator.evaluate(train_preds)

    val resultTestPreds = evaluator.evaluate(test_preds)

    println(s"Score for Train $resultTrainPreds, Score for Test $resultTestPreds")

    // Printing runtime duration in minutes
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Run time: $duration")
  }
}

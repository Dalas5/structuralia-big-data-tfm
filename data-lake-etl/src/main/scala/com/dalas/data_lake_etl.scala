package com.dalas

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, DateType}
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.SparkSession
import java.sql.Date

object data_lake_etl {

  case class Review(marketplace: String, customer_id: Int, review_id: String, product_id: String,
                    product_parent: Int, product_title: String, product_category: String,
                    star_rating: String, helpful_votes: Int, total_votes: Int, vine: String,
                    verified_purchase: String, review_headline: String, review_body: String, review_date: Date)


  def main(args: Array[String]): Unit = {

    // origin data sourcce folder: /home/dalas/Downloads/archive , files are in tsv format inside folder

    val archive_path = "/home/dalas/Downloads/archive"
    val project_archive_base_path = "s3a://amazon/data/archive" // path to store original data as hudi table
    val project_eda_base_path = "s3a://amazon/data/eda-results" // path to store EDA results

    // additional bucket names probably
    // s3a://amazon/ml/nlp
    // s3a://amazon/ml/cluster

    val startdt = "1998-07-15"
    val enddt = "1999-12-31"

    val hudiTableName = "amazon_reviews"


    val spark = SparkSession.builder()
      .appName("Data Lake Ingestion ETL")
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

//    val tripsSnapshotDF = spark.
//      read.
//      format("hudi").
//      load(basePath)
//
//    tripsSnapshotDF.show()


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
      .csv(archive_path)
      .as[Review]

    // Safely casting star_rating from string to int
    val intRatingAmazonDF = amazonDF
      .withColumn("int_star_rating",
        when(col("star_rating").isNotNull, udfInt(col("star_rating"))).otherwise(lit(null)))
      .filter(col("review_id").isNotNull)
      .where(col("review_date").between(startdt, enddt))

    // intRatingAmazonDF.createOrReplaceTempView("initial_amazon")

    // Finding min and max date
    // intRatingAmazonDF.printSchema()

    /*
    root
     |-- marketplace: string (nullable = true)
     |-- customer_id: integer (nullable = true)
     |-- review_id: string (nullable = true)
     |-- product_id: string (nullable = true)
     |-- product_parent: integer (nullable = true)
     |-- product_title: string (nullable = true)
     |-- product_category: string (nullable = true)
     |-- star_rating: string (nullable = true)
     |-- helpful_votes: integer (nullable = true)
     |-- total_votes: integer (nullable = true)
     |-- vine: string (nullable = true)
     |-- verified_purchase: string (nullable = true)
     |-- review_headline: string (nullable = true)
     |-- review_body: string (nullable = true)
     |-- review_date: date (nullable = true)
     |-- int_star_rating: integer (nullable = true)

     */

//    val min_date = intRatingAmazonDF.select(min("review_date")).head().toString()
//    val max_date = intRatingAmazonDF.select(max("review_date")).head().toString()
//    println(s"The minimum date is: $min_date")
//    println(s"The minimum date is: $max_date")

    /*
    The minimum date is: [1998-07-15]
    The minimum date is: [2015-08-31]
     */

    // val initialRowCount = intRatingAmazonDF.count()
    // println(s"Count of initial DF: $initialRowCount")
    // Count of initial DF: 15,597,549   ; after null removal: 15,597,548

    // val reviewIdCount = intRatingAmazonDF.select(countDistinct("review_id")).collect()(0)(0)
    // println(s"Count of review_id : $reviewIdCount")
    // Count of review_id : 15,597,548    ;   count of not null review_id 15,597,548

    // After review_id null removal, counts match. review_id field it's ready to be used as RECORDKEY for hudi

  /*
  -RECORD 3----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   marketplace       | US
   customer_id       | 52782374
   review_id         | R1PR37BR7G3M6A
   product_id        | B00D7H8XB6
   product_parent    | 868449945
   product_title     | AmazonBasics 12-Sheet High-Security Micro-Cut Paper
   product_category  | Office Products
   star_rating       | 1
   helpful_votes     | 2
   total_votes       | 3
   vine              | N
   verified_purchase | Y
   review_headline   | and the shredder was dirty and the bin was partially full of shredded paper
   review_body       | Although this was labeled as &#34;new&#34; the one I received clearly had been used. The box had previously been opened., and the shredder was dirty and the bin was partially full of shredded paper. What was worse is that the unit will not work properly. It is not possible to insert the paper bin so as to enable the shredder to run. It will not operate if the bin is not in place, but I could never get the unit to recognize that the paper bin was actually fully inserted. After cleaning everything thoroughly and vacuuming the paper bin area, it worked ONCE! After that I was unable to get it work at all. I returned the unit immediately for a refund. I feel Amazon misrepresented the  unit as &#34;new&#34; when clearly it was not.
   review_date       | 2015-08-31
   int_star_rating   | 1
   */

    // TODO: Save dataframe to s3 bucket

    val finalRatingAmazonDF = intRatingAmazonDF.withColumn("updated_ts", current_timestamp())

    finalRatingAmazonDF.write.format("hudi")
      .option(RECORDKEY_FIELD, "review_id")
      .option(PARTITIONPATH_FIELD, "review_date")
      .option(OPERATION, INSERT_OPERATION_OPT_VAL)
      .option(TABLE_TYPE, COW_TABLE_TYPE_OPT_VAL)
      .option(PRECOMBINE_FIELD, "updated_ts")
      .option(TABLE_NAME, hudiTableName)
      .mode(Overwrite)
      .save(project_archive_base_path)


  }
}

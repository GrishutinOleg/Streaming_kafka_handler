import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import breeze.linalg.{DenseVector, inv}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}



object Stream_handler extends App {

  val spark = SparkSession
    .builder()
    .appName("Streaming kafka handler")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topicinput = "items_translated"

  val itemsschema = new StructType()
    //.add("Datecur", StringType)
    //.add("Timecur", StringType)
    .add("Curts", LongType)
    .add("X", IntegerType)
    .add("Y", IntegerType)
    .add("Z", IntegerType)

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topicinput)
    .option("startingOffsets", "latest")
    //.option("startingOffsets", "earliest")
    .load()


  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
  //.cache()
  //.localCheckpoint()

  val df3 = df1.select(from_json(col("value"), itemsschema).as("data"))
    .select("data.*")
    .withColumn(
      "timestamp_mili",
      (col("Curts")/1000).cast("timestamp"))

  val windowedData = df3
    .withWatermark("timestamp_mili","1 minute")
    .groupBy(
      window(col("timestamp_mili"), "2 minutes", "1 minute").as("window"),
      //$"Curts",$"timestamp_mili", $"X", $"Y", $"Z"
      col("Curts"), col("timestamp_mili"), col("X"), col("Y"), col("Z")
    ).count()
    .selectExpr("window.*", "Curts", "timestamp_mili", "X", "Y", "Z", "count")

  //df3.printSchema()

  windowedData.printSchema()
///*
  windowedData.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>

    println(s"batch id is: ${batchId}")
    batchDF.show()
    /*
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y", "Z")).setOutputCol("vector")
    val standardScalar = new StandardScaler().setInputCol("vector").setOutputCol("normalized-vector").setWithMean(true)
      .setWithStd(true)

    val pca = new PCA().setInputCol("normalized-vector").setOutputCol("pca-features").setK(2)

    val pipeline = new Pipeline().setStages(
      Array(vectorAssembler, standardScalar, pca)
    )

    val pcaDF = pipeline.fit(batchDF).transform(batchDF)

    def withMahalanobois(df: DataFrame, inputCol: String): DataFrame = {
      val Row(coeff1: Matrix) = Correlation.corr(df, inputCol).head

      val invCovariance = inv(new breeze.linalg.DenseMatrix(2, 2, coeff1.toArray))

      val mahalanobois = udf[Double, Vector] { v =>
        val vB = DenseVector(v.toArray)
        vB.t * invCovariance * vB
      }

      df.withColumn("mahalanobois", mahalanobois(df(inputCol)))
    }

    val withMahalanoboisDF: DataFrame = withMahalanobois(pcaDF, "pca-features")

    val withmarked = withMahalanoboisDF.withColumn("annom-flag", when(col("mahalanobois") > 3, 1).otherwise(0))

    val calculateddata = withmarked.select("Curts", "X", "Y", "Z", "annom-flag")

    calculateddata.show()

    //val annomdata = calculateddata.where(calculateddata("annom-flag") === 1)

    //annomdata.show()
*/

  }
    .option("checkpointLocation", "/tmp/spark-streaming-pca-writing/checkpoint-handler")
    .start()
    .awaitTermination()
//*/
/*
  val df4 =
    //df3
    windowedData
  //calculateddata
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
*/


}

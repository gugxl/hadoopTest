package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo14 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo14").master("local").getOrCreate()
        val path = "file:///D:\\applicationfiles\\data\\iris.data.txt"
        val df_raw: DataFrame = spark.read
            .option("inferSchema", true)
            .csv(path)
            .toDF("c0", "c1", "c2", "c3", "label")
        val df_double: DataFrame = df_raw.select(
            col("c0").cast(DoubleType),
            col("c1").cast(DoubleType),
            col("c2").cast(DoubleType),
            col("c3").cast(DoubleType),
            col("label"))
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("c0", "c1", "c2", "c3"))
            .setOutputCol("features")
        
        val df: DataFrame = assembler.transform(df_double).select("features")
        val kmeansmodel: KMeansModel = new KMeans()
            .setK(3)
            .setFeaturesCol("features")
            .setPredictionCol("predict")
            .fit(df)
        val result: DataFrame = kmeansmodel.transform(df)
        result.collect().foreach(row => {
            println(row(0) + " =>  cluster " + row(1))
        })
        
        kmeansmodel.clusterCenters.foreach(center => {
            println("Clustering Center:" + center)
        })
        
        val evaluator = new ClusteringEvaluator()
        val silhouette: Double = evaluator.evaluate(result)
        println(silhouette)
        
    }
    
}

package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo15 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo15").master("local").getOrCreate()
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
        val gm: GaussianMixture = new GaussianMixture()
            .setK(3)
            .setPredictionCol("predict")
            .setProbabilityCol("Probability")
        
        val gmm: GaussianMixtureModel = gm.fit(df)
        val result: DataFrame = gmm.transform(df)
        result.show(150, false)
        for (i <- 0 until gmm.getK) {
            println("Component %d : \n weight: %f \n mu vector: \n %s \n sigma matrix: \n %s \n "
                format(i, gmm.weights(i), gmm.gaussians(i).mean, gmm.gaussians(i).cov))
        }
        
        
    }
    
}

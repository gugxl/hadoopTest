package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

// 特征选择
object Demo11 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo11").master("local").getOrCreate()
        val data: Seq[(Int, linalg.Vector, Int)] = Seq((1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1),
            (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0),
            (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0))
        
        val df: DataFrame = spark.createDataFrame(data).toDF("id", "features", "label")
        df.show()
        
        val selector: ChiSqSelector = new ChiSqSelector()
            .setNumTopFeatures(1)
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setOutputCol("selected-feature")
        
        val selector_model: ChiSqSelectorModel = selector.fit(df)
        val result: DataFrame = selector_model.transform(df)
        result.show(false)
    }
    
}

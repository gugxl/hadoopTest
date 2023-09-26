package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.feature.{VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo10 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo10").master("local").getOrCreate()
        val data: Seq[linalg.Vector] = Seq(
            Vectors.dense(-1.0, 1.0, 1.0),
            Vectors.dense(-1.0, 3.0, 1.0),
            Vectors.dense(0.0, 5.0, 1.0))
        
        val df: DataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
        val indexer: VectorIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexed")
            .setMaxCategories(2) // 即只有种类小于2的特征才被认为是类别型特征，否则被认为是连续型特征。
        
        val indexerModel: VectorIndexerModel = indexer.fit(df)
        val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
        
        println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))
        indexerModel.transform(df).show()
    }
    
}

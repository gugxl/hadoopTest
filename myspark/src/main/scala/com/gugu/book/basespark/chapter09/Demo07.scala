package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.Summarizer.{mean, metrics, variance}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Demo07 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo05").master("local").getOrCreate()
        
        val data = Seq((0.0, Vectors.dense(3.5, 40.0)),
            (0.0, Vectors.dense(3.5, 30.0)),
            (1.0, Vectors.dense(1.5, 30.0)),
            (0.0, Vectors.dense(1.5, 20.0)),
            (0.0, Vectors.dense(0.5, 10.0)))
        import spark.implicits._
        val df: DataFrame = data.toDF("label", "features")
        val chi: Row = ChiSquareTest.test(df, "features", "label").head
        println(s"pValues = ${chi.getAs[Vector](0)}")
        println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
        println(s"statistics ${chi.getAs[Vector](2)}")
        val data2 = Seq(
            (Vectors.dense(1.0, 2.0, 4.0), 1.0),
            (Vectors.dense(4.0, 3.0, 6.0), 3.0))
        val df2: DataFrame = data2.toDF("features", "weight")
        val (meanVal, varianceVal) = df2.select(metrics("mean", "variance")
            .summary($"features", $"weight").as("summary"))
            .select("summary.mean", "summary.variance")
            .as[(Vector, Vector)].first()
        println(meanVal)
        println(varianceVal)
        
        val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
            .as[(Vector, Vector)].first()
        println(meanVal2)
        println(varianceVal2)
    }
    
}

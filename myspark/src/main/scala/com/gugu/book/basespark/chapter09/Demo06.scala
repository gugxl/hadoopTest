package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

object Demo06 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo05").master("local").getOrCreate()

        val data = Seq(
            Vectors.sparse(4,
            Seq((0, 2.0), (2, -1.0))),
            Vectors.dense(3.0, 0.0, 4.0, 5.0),
            Vectors.dense(6.0, 8.0, 0.0, 7.0))
        import spark.implicits._
        val df = data.map(Tuple1.apply).toDF("features")
        val row: Row = Correlation.corr(df, "features").head()
        val head: Row = Correlation.corr(df, "features", "spearman").head
        println(df)
        println(row)
        println(head)
    }
}

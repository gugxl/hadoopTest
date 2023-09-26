package com.gugu.book.basespark.chapter09

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.fpm.FPGrowth

object Demo17 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo17").master("local").getOrCreate()
        import spark.implicits._
        val smallTestData: Seq[Seq[Seq[Int]]] = Seq(Seq(Seq(2), Seq(1), Seq(3)),
            Seq(Seq(3), Seq(1, 2)),
            Seq(Seq(1), Seq(2, 3), Seq(2, 4)),
            Seq(Seq(3), Seq(5)))
        val df: DataFrame = smallTestData.toDF("sequence")
//        val model = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5).setMaxLocalProjDBSize(32000000)
//
//        val result =  model.findFrequentSequentialPatterns(df)
//        result.show()
    }
    
}

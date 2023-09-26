package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo09 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo09").master("local").getOrCreate()
        val df1: DataFrame = spark.createDataFrame(Seq(
            (0, "a"),
            (1, "b"),
            (2, "c"),
            (3, "a"),
            (4, "a"),
            (5, "c"))).toDF("id", "category")
        //        字符标签映射索引列
        val indexer: StringIndexer = new StringIndexer()
            .setInputCol("category")
            .setOutputCol("categoryIndex")
        val indexed1: DataFrame = indexer.fit(df1).transform(df1)
        indexed1.show()
        //        索引映射标签
        val idx2str: IndexToString = new IndexToString()
            .setInputCol("categoryIndex")
            .setOutputCol("originalCategory")
        val indexString: DataFrame = idx2str.transform(indexed1)
        indexString.select("id", "originalCategory").show
        
    }
    
}

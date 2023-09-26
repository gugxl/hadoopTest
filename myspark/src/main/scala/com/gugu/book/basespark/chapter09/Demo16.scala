package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo16 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo16").master("local").getOrCreate()
        import spark.implicits._
        val dataset: DataFrame = spark.sparkContext
            .textFile("file:///D:\\applicationfiles\\data\\sample_fpgrowth.txt")
            .map(t => t.split(" "))
            .toDF("items")
        dataset.show(false)
        
        val fpgrowth: FPGrowth = new FPGrowth()
            .setItemsCol("items")
            .setMinSupport(0.5)
            .setMinConfidence(0.6)
        
        val model: FPGrowthModel = fpgrowth.fit(dataset)
        //        输出频繁模式集
        model.freqItemsets.show()
        //        关联规则
        model.associationRules.show()
        
        model.transform(dataset).show()
    }
    
}

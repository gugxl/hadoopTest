package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo08 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo08").master("local").getOrCreate()
        //        创建一个集合，每一个句子代表一个文件
        val sentenceData = spark.createDataFrame(Seq(
            (0, "I heard about Spark and I love Spark"),
            (0, "I wish Java could use case classes"),
            (1, "Logistic regression models are neat")
        )).toDF("label", "sentence")
        //        用Tokenizer把每个句子分解成单词。
        val tokenizer: Tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData: DataFrame = tokenizer.transform(sentenceData)
        wordsData.show(false)
        //        用HashingTF的transform()方法把每个“词袋”哈希成特征向量。这里设置哈希表的桶数为2000。
        val hashingTF: HashingTF = new HashingTF()
            .setInputCol("words")
            .setOutputCol("rawFeatures")
        val featurizedData: DataFrame = hashingTF.transform(wordsData)
        featurizedData.select("words", "rawFeatures")
            .show(false)
        //        调用IDF方法来重新构造特征向量的规模，生成的变量idf是一个评估器，在特征向量上应用它的fit()方法，会产生一个IDFModel（名称为idfModel）
        val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val iDFModel: IDFModel = idf.fit(featurizedData)
        val rescaledData: DataFrame = iDFModel.transform(featurizedData)
        rescaledData.select("features", "label").show(false)
    }
    
    
}

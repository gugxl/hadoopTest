package com.gugu.book.basespark.chapter09

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object Demo02 {
    def main(args: Array[String]): Unit = {
        val pos: LabeledPoint = LabeledPoint(1.0, Vectors.dense(2.0, 0.0, 8.0))
        val neg: LabeledPoint = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0)))
        
    }
    
}

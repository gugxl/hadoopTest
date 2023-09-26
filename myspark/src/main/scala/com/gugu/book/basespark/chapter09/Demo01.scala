package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors

object Demo01 {
    def main(args: Array[String]): Unit = {
        val dv: linalg.Vector = Vectors.dense(2.0, 0.0, 8.0)
        val sv1: linalg.Vector = Vectors.sparse(3, Array(0, 2), Array(2.0, 8.0))
        val sv2: linalg.Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))
    }
    
}

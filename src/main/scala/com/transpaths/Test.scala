package com.transpaths

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Test1 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first spark").setMaster("local")
    // 假设 SparkContext 已经被构建
    val sc: SparkContext = new SparkContext(conf)
    // 为 vertices 创建一个 RDD
    val trans = sc.parallelize(Array((2L, Set(List(((2,3),(124,100.0))), List(((2,3),(124,100.0)), ((3,4),(125,100.0))))),
                                     (1L, Set(List(((1,8),(123,100.0))), List(((1,2),(123,100.0)), ((2,3),(124,100.0))), List(((1,2),(123,100.0)), ((2,3),(124,100.0)), ((3,4),(125,100.0)))))
                                    ))
//    trans.map(kv => {
//      kv._2
//    }).collect.foreach(println)

    trans.flatMapValues(v => {
      val r = mutable.Map[((Int,Int),(Int,Double)),Set[List[((Int,Int),(Int,Double))]]]()
      v.foreach( l => {
        var p:Set[List[((Int,Int),(Int,Double))]] = r.getOrElse(l(0), Set[List[((Int,Int),(Int,Double))]]())
        p = p + l
        r.put(l(0), p)
      })
      r.toList
    }
    ).collect().foreach(println)

  }
}



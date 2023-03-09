package com.transpaths

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object TransInfo {

  def main(args: Array[String]): Unit = {

    //调用 spark-submit --master local --class TransPath.TransInfo  graphx-1.0-SNAPSHOT.jar

    //创建运行环境
    val conf = new SparkConf().setAppName("TransInfo-GraphX").setMaster("local")


    //读取json文件
    //val spark = SparkSession.builder().config(conf).getOrCreate()
    //val df = spark.read.json("data/data.json")
    //val trans: RDD[Edge[(Long, Double)]] = df.select("src_id", "dst_id", "deal_time", "deal_money").rdd.map(r => Edge(r.get(0).toString.toLong, r.get(1).toString.toLong, (transTimeToLong(r.get(2).toString), r.get(3).toString.toDouble)))

    //读取Hive数据库
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val hrdd: RDD[Row] = spark.sql("select src_id, dst_id, deal_time, deal_money from transaction where partition_dt=20220101").rdd
    val trans: RDD[Edge[(Long, Double)]] = hrdd.map(r => Edge(r.get(0).toString.toLong, r.get(1).toString.toLong, (transTimeToLong(r.get(2).toString), r.get(3).toString.toDouble)))

    //通过边数据构建图
    val graph = Graph.fromEdges(trans, 0)

    //初始信息
    val initialMsg: Set[List[((VertexId, VertexId), (Long, Double))]] = Set()

    //设置查询深度
    val maxDepth = 5

    //初始化图，把节点信息进行初始化为集合对象
    val initialGraph = graph.mapVertices((id, _) => Set[List[((VertexId, VertexId), (Long, Double))]]())

    //调用pregel算法，查询出每个节点的下游交易链路
    val pathGraph = initialGraph.pregel(initialMsg, maxDepth, EdgeDirection.In)(

      // 更新顶点信息， 把列表进行合并
      (id, path, newPaths) => {
        println("v: id:" + id + ",path:" + path + ",newpath:" + newPaths);
        path ++ newPaths
      },

      // 向活跃节点发送消息
      triplet => {
        println("sendmsg:" + triplet.srcId + "->" + triplet.dstId)

        if (triplet.dstAttr.size < maxDepth) {
          println("sendmsg:" + triplet.srcId + "->" + triplet.srcAttr + "+" + triplet.dstId)
          var res: Set[List[((VertexId, VertexId), (Long, Double))]] = Set()

          if (triplet.dstAttr.size == 0) {
            val msg: List[((VertexId, VertexId), (Long, Double))] = ((triplet.srcId, triplet.dstId), triplet.attr) :: Nil
            res = res + msg
            Iterator((triplet.srcId, res))
          }
          else {
            triplet.dstAttr.foreach(a => {
              //增加判断条件，判断交易金额在指定范围内，避免金额过小，或过大

              //增加判断条件，如果时间小于后续交易，纳入交易链路。
              if (a(0)._2._1 > triplet.attr._1) {
                //把数据加入到列表当中
                val msg: List[((VertexId, VertexId), (Long, Double))] = ((triplet.srcId, triplet.dstId), triplet.attr) :: Nil ::: a
                res = res + msg
              }
            })
            Iterator((triplet.srcId, res))
          }
        }
        else {
          Iterator.empty
        }
      },
      //指向相同顶点的边，进行合并操作
      (a, b) => {
        println("merge:" + a + "," + b);
        a ++ b
      }
    )


    //    pathGraph.vertices.map(kv => {
    //
    //      val set = kv._2
    //      val nodeId = kv._1
    //      //所有路径信息进行分组，存储到map集合中，根据起始节点key进行查找
    //      val allPath: Map[Long, Set[(Long, (Long, Long, Double))]] = set.flatten.map(a => (a._1._1, (a._1._2, a._2._1, a._2._2))).groupBy(a => a._1)
    //
    //      println("allPath:" + allPath)
    //      val initPaths = allPath.getOrElse(nodeId, Nil)
    //
    //      val result = ListBuffer[List[List[(Int, Long, Long, Long, Double)]]]()
    //
    //      for (iter <- initPaths.iterator) {
    //        println("初始化节点:" + iter)
    //        val initNode = NodeInfo(0, iter._1, iter._2._1, iter._2._2, iter._2._3)
    //        val r = fromOnePath(initNode, allPath)
    //        result += r
    //      }
    ////      (4,)
    ////      (1,List(List((0,1,2,123,100.0), (1,2,3,124,100.0), (2,3,4,125,100.0))))
    ////      (3,List(List((0,3,4,125,100.0))))
    ////      (2,List(List((0,2,3,124,100.0), (1,3,4,125,100.0))))
    //      (nodeId,result.toList)
    //    }).collect.foreach(println)
    //将顶点数据与下游交易数据拆分开，每条交易一行记录
    val paths = pathGraph.vertices.flatMapValues(v => {
      val r = mutable.Map[((Long, Long), (Long, Double)), Set[List[((Long, Long), (Long, Double))]]]()
      v.foreach(l => {
        //把所有交易链路的第一个值取出，当作key存储，然后把key相同的，所有的列表值进行追加到一个Set中
        var p: Set[List[((Long, Long), (Long, Double))]] = r.getOrElse(l(0), Set[List[((Long, Long), (Long, Double))]]())
        p =  p + l
        r.put(l(0), p)
      })
      r.values
    }
    ).map( kv => {
        val set = kv._2
        val nodeId = kv._1
        println("values:"+set)
        //把所有路径信息进行分组，存储到map集合中，根据起始节点key进行查找
        val allPath: Map[Long, Set[(Long, (Long, Long, Double))]] = set.flatten.map(a => (a._1._1, (a._1._2, a._2._1, a._2._2))).groupBy(a => a._1)

        println("allPath:" + allPath)
        val initPaths = allPath.getOrElse(nodeId, Nil)

        var r :List[List[(Int, Long, Long, Long, Double)]] = Nil

        for (item <- initPaths.iterator) {
          println("初始化节点:" + item)
          val initNode = NodeInfo(0, item._1, item._2._1, item._2._2, item._2._3)
          r = fromOnePath(initNode, allPath)
        }
        (nodeId, r.mkString("|"))
      }
    )
    //(1111111116,List((0,1111111116,1111111117,1641042005000,10000.0)))

    val schema: types.StructType = StructType(
      Seq(
        StructField("src_id",LongType,true),
        //StructField("result",StringType,true),
      )
    )
    val rowRDD: RDD[Row] = paths.map(r => Row(r._1))

    val rowDF = spark.createDataFrame(rowRDD,schema)

    rowDF.createOrReplaceTempView("trans")

    spark.table("trans").write.insertInto("transresult")

    //关闭
    spark.stop()
  }

  def transTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }


  /**
   * 从一笔交易出发，查询这笔交易的链路组合数据
   *
   * @param initNode
   * @param allPath
   */
  def fromOnePath(initNode: NodeInfo, allPath: Map[Long, Set[(Long, (Long, Long, Double))]]): List[List[(Int, Long, Long, Long, Double)]] = {

    //初始化信息
    val transMain = TransMain()
    val transPath = TransPath()

    var pathList: ListBuffer[List[(Int, Long, Long, Long, Double)]] = ListBuffer()

    //条件信息
    transMain.limit = new LimitElement()

    //初始交易进入队列
    transPath.queue.enqueue(initNode)

    //把交易路径类入栈
    transMain.pathStack.push(transPath)

    //交易路径不为空，表示还有组合的交易，继续寻找
    while (transMain.pathStack.nonEmpty) {
      val p: TransPath = transMain.pathStack.pop()
      val res = transMain.findPath(p, allPath)

      val rl = res.flatten.map(lst => {
        for (idx <- 0 until lst.cardId.size)
          yield (lst.level, lst.cardId(idx), lst.toCardId, lst.dealTime(idx), lst.dealMoney(idx))
      }).flatten

      pathList.append(rl.toList)
    }
    println("onepath=" + pathList)
    pathList.toList
  }
}

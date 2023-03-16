package com.transpaths

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TransPath extends Serializable {

  //创建队列存储交易层次信息，对数据进行广度优先搜索
  var queue: mutable.Queue[NodeInfo] = new mutable.Queue[NodeInfo]()

  //初始化 消息列表，用于存储结果数据，每一层一个列表
  var info: ListBuffer[List[NodeInfo]] = ListBuffer()

  //交易尾部节点信息
  var endTransList: ListBuffer[NodeInfo] = ListBuffer()

  //记录层级列表信息
  var levelList: ListBuffer[NodeInfo] = ListBuffer()

  //层级号
  var levelNum: Int = 1

  //记录当前层级队列弹出了多少
  var usedNum: Int = 0

  var queueNum: Int = 0

  //是否是组合标记
  var combineFlag: Boolean = false

  def this(queue: mutable.Queue[NodeInfo], info: ListBuffer[List[NodeInfo]], endTransList: ListBuffer[NodeInfo], levelList: ListBuffer[NodeInfo], levelNum: Int, usedNum: Int, queueNum: Int, combineFlag: Boolean) {
    this()
    this.queue = queue
    this.info = info
    this.endTransList = endTransList
    this.levelList = levelList
    this.levelNum = levelNum
    this.usedNum = usedNum
    this.queueNum = queueNum
  }

  override def toString: String = this.info.mkString(",")

}

object TransPath {
  def apply(): TransPath = new TransPath()
}

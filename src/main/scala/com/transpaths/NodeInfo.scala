package com.transpaths

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

class NodeInfo {

  @BeanProperty var level = 0
  @BeanProperty val cardId: ListBuffer[Long] = ListBuffer()
  @BeanProperty var toCardId: Long = 0L
  @BeanProperty val dealTime: ListBuffer[Long] = ListBuffer()
  @BeanProperty val dealMoney: ListBuffer[Double] = ListBuffer()
  @BeanProperty var maxDealTime = 0L
  @BeanProperty var totalMoney: Double = _


  def this(level:Int, cardId:Long, toCardId:Long, dealTime:Long, money: Double){
    this()
    this.level = level
    this.cardId.append(cardId)
    this.toCardId = toCardId
    this.dealTime.append(dealTime)
    this.dealMoney.append(money)
    this.maxDealTime = dealTime
    this.totalMoney = money
  }

  //删除相同交易信息
  def delInfo(card: Long, time: Long, money: Double): Boolean = {

    for(index <- 0 until cardId.size){
      if(cardId(index) == card && dealTime(index) == time && dealMoney(index) == money){
        cardId.remove(index)
        dealTime.remove(index)
        dealMoney.remove(index)
        return true
      }
    }
    false
  }

  override def toString: String = "["+this.level+","+this.cardId+","+this.toCardId+","+this.dealTime+","+this.dealMoney+","+this.maxDealTime+","+this.totalMoney

}

object NodeInfo{
  def apply: NodeInfo = new NodeInfo()

  def apply(level:Int, cardId:Long, toCardId:Long, dealTime:Long, money: Double): NodeInfo = {
    new NodeInfo(level, cardId, toCardId, dealTime, money)
  }
}
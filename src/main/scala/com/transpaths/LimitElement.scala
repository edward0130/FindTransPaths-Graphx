package com.transpaths

import scala.beans.BeanProperty

class LimitElement extends Serializable {
  @BeanProperty var minScale = 0.8f
  @BeanProperty var maxScale = 1.2f
  @BeanProperty var minMoney = 1000
  @BeanProperty var singleScale = 0.1f
  @BeanProperty var combineNum = 3
  @BeanProperty var maxDepth = 5
  @BeanProperty var hours = 6

  def this(minScale:Float, maxScale:Float, minMoney:Int, singleScale:Float, combineNum:Int, maxDepth:Int, hours:Int){
    this()
    this.minScale = minScale
    this.maxScale = maxScale
    this.minMoney = minMoney
    this.singleScale = singleScale
    this.combineNum = combineNum
    this.maxDepth = maxDepth
    this.hours = hours
  }

  def getMillisecond(): Long = this.hours * 60 * 60 * 1000

}

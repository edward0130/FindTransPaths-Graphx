package com.transpaths

import scala.beans.BeanProperty

class LimitElement {
  @BeanProperty var minScale = 0.8f
  @BeanProperty var maxScale = 1.2f
  @BeanProperty var combineNum = 3
  @BeanProperty var singleScale = 0.1f
  @BeanProperty var steps = 0
  @BeanProperty var hours = 24
  @BeanProperty var minMoney = 1000

  def getMillisecond(): Long = this.hours * 60 * 60 * 1000
}

package com.transpaths

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


//object TransTest{
//  def main(args: Array[String]): Unit = {
//
//    //测试数据：
//    //val set:Set[List[((Long,Long),(Int,Int))]] = Set(List(((2,8),(4,100))), List(((2,3),(1,100)), ((3,5),(2,200))), List(((2,8),(4,100)), ((8,9),(5,200))), List(((2,3),(1,100))), List(((2,3),(1,100)), ((3,5),(2,200)), ((5,7),(3,130))), List(((2,3),(1,100)), ((3,5),(2,200)), ((5,7),(3,130)), ((7,2),(5,200))))
//
//    //测试数据，组合交易
//    //val set:Set[List[((Long,Long),(Int,Int))]] = Set(List(((1,2),(1,300))), List(((2,3),(2,200))), List(((2,4),(3,100))), List(((2,5),(4,100))), List(((3,6),(5,200))), List(((3,7),(6,200))))
//    //val set:Set[List[((Long,Long),(Int,Int))]] = Set(List(((1,2),(1,300))), List(((1,2),(2,300))),List(((2,3),(3,200))), List(((2,4),(4,100))), List(((2,5),(5,100))), List(((3,6),(6,200))), List(((3,7),(7,200))))
//    val set:Set[List[((Long,Long),(Long,Double))]] = Set(List(((1,2),(1,300))), List(((1,2),(2,300))),List(((2,3),(3,200))), List(((2,4),(4,100))), List(((2,5),(5,100))), List(((3,6),(6,200))), List(((3,7),(7,200))),List(((2,7),(8,100))))
//
//    val nodeId = 1L
//    //所有路径信息进行分组，存储到map集合中，根据起始节点key进行查找
//    val allPath:Map[Long, Set[(Long, (Long, Long, Double))]] = set.flatten.map( a=> (a._1._1, (a._1._2, a._2._1, a._2._2))).groupBy(a => a._1)
//
//    println("allPath:"+allPath)
//    val initPaths = allPath.getOrElse(nodeId, Nil)
//
//    val result= ListBuffer[List[List[(Int, Long, Long, Long, Double)]]]()
//
//    for (iter <- initPaths.iterator) {
//      println("初始化节点:"+iter)
//      val initNode = NodeInfo(0, iter._1, iter._2._1, iter._2._2, iter._2._3)
//      val r = fromOnePath(initNode, allPath)
//      result += r
//    }
//    println("result="+result)
//  }
//
//
//  /**
//    * 从一笔交易出发，查询这笔交易的链路组合数据
//    *
//    * @param initNode
//    * @param allPath
//    */
//  def fromOnePath(initNode: NodeInfo, allPath: Map[Long, Set[(Long, (Long, Long, Double))]]):List[List[(Int, Long, Long, Long, Double)]] ={
//
//    //初始化信息
//    val transMain = TransMain()
//    val transPath = TransPath()
//
//    var pathList: ListBuffer[List[(Int, Long, Long, Long, Double)]] = ListBuffer()
//
//    //条件信息
//    transMain.limit = new LimitElement()
//
//    //初始交易进入队列
//    transPath.queue.enqueue(initNode)
//
//    //把交易路径类入栈
//    transMain.pathStack.push(transPath)
//
//    //交易路径不为空，表示还有组合的交易，继续寻找
//    while(transMain.pathStack.nonEmpty){
//      val p:TransPath = transMain.pathStack.pop()
//      val res = transMain.findPath(p, allPath)
//
//      val rl = res.flatten.map(lst => { for( idx <- 0 until lst.cardId.size)
//        yield (lst.level, lst.cardId(idx), lst.toCardId, lst.dealTime(idx), lst.dealMoney(idx))
//      }).flatten
//
//      pathList.append(rl.toList)
//    }
//    println("onepath="+pathList)
//    pathList.toList
//  }
//}


class TransMain{

  var limit:LimitElement = _
  var pathStack = mutable.Stack[TransPath]()

  /**
    * 查找交易路径，使用广度优先算法
    * @param transPath
    */
  def findPath(transPath: TransPath, allPath: Map[Long, Set[(Long, (Long, Long, Double))]]): ListBuffer[List[NodeInfo]] ={

    while(transPath.queue.nonEmpty){

      //获取当前层级的交易数量
      var count = transPath.queue.size

      //创建层级列表，存储层级交易记录
      var levelList = ListBuffer[NodeInfo]()

      //如果是组合路径，复原初始状态
      if(transPath.combineFlag == true){
        count = transPath.queueNum
        levelList = transPath.levelList
        transPath.combineFlag = false
      }

      //循环处理当前层级的节点
      for(i <- 0 until count ){

        //从队列中取出一个节点
        val node:NodeInfo = transPath.queue.dequeue()

        //将取出的节点的起始账号与所有层级的尾部节点的目标账号进行比较，如果账号相同，将交易金额累加到当前节点
        //解决场景如下：
        //      1 -> 2 -> 3
        //      1    ->   3
        sumEndTransInfo(node, transPath.endTransList)

        //将取出的节点与历史节点进行比较，如果有相同的交易，则不进行记录, 否则继续
        val check:Boolean = checkSameTransInfo(node, transPath.info)
        if (check == false){

          //把从队列中取出的信息存储到这个层级的列表中
          levelList.append(node)

          //基于取出的节点查找有关系的交易节点
          var nodeList: ListBuffer[NodeInfo] = getTransInfo(node, transPath.levelNum, transPath.queue, transPath.endTransList, allPath)

          //单笔交易
//          if (nodeList.size == 1){
//            transPath.queue.enqueue(nodeList(0))
//          }

          //组合交易
          if(nodeList.size >= 1){

            val target = node.totalMoney
            //组合序号列表结果集
            val allCombine:List[List[Int]] = findCombineTrans(nodeList, target)
            //println("组合结果："+allCombine)
            if(allCombine.size==0)
              nodeList = null

            //多种组合把组合记录下来，放到堆栈中,从第二个组合开始进行拷贝
            for ( j <- 1 until allCombine.size) {

              //路径拷贝，拷贝后放入到堆栈中
              transPath.levelList = levelList;
              val bt: TransPath = transPath.clone();
              //设置组合标记
              bt.combineFlag = true;
              //记录当前层级剩余的队列数量
              bt.queueNum = count - i - 1;
              for (k <- 0 until allCombine(j).size) {
                if (!unionTransInfo(nodeList, nodeList(allCombine(j)(k)), bt.queue))
                  bt.queue.enqueue(nodeList(allCombine(j)(k)))
              }
              //把组合路径放到堆栈中，
              pathStack.push(bt)
            }
            //把组合的第一个路径写入队列
            if (allCombine.size>0) {
              for(k <- 0 until allCombine(0).size){
                if (!unionTransInfo(nodeList, nodeList(allCombine(0)(k)), transPath.queue))
                  transPath.queue.enqueue(nodeList(allCombine(0)(k)))
              }
            }
          }
        }
      }
      transPath.levelNum+=1
      //层级信息加入到列表结果集中
      transPath.info.append(levelList.toList)
    }
    //println("transPath:"+transPath.toString)
    transPath.info
  }

  /**
    * 对目标相同的账号交易总和进行合并
    *
    * @param node
    * @param endTransList
    */

  def sumEndTransInfo(node:NodeInfo, endTransList:ListBuffer[NodeInfo]): Unit ={

    //方法一
    for(iterator <- endTransList.iterator){
      if(node.toCardId == iterator.toCardId){
        val money = node.getTotalMoney + iterator.getTotalMoney
        node.setTotalMoney(money)
        endTransList.-(iterator)
        println("endTransList"+endTransList)
      }
    }


//    //方法二：通过索引删除节点
//    for(index <- 0 until endTransList.size){
//      if(node.toCardId == endTransList(index).toCardId){
//        val money = node.getTotalMoney + endTransList(index).getTotalMoney
//        node.setTotalMoney(money)
//        endTransList.remove(index)
//        return
//      }
//    }
  }


  /**
    * 检查是否有相同的交易，如果有则过滤
    *
    * @param node
    * @param info
    * @return
    */
  def checkSameTransInfo(node: NodeInfo, info: ListBuffer[List[NodeInfo]]):Boolean={
    //节点与历史层级进行比较
    for(iterList <- info){
      for(iter <- iterList){
        for(index <- 0 until iter.cardId.size){
          node.delInfo(iter.cardId(index), iter.dealTime(index), iter.dealMoney(index))
        }
      }
    }

    if(node.cardId.size==0){
      return true;
    }
    false
  }


  /**
    * 简单条件过滤
    * 3.组合交易单笔金额占比， 不能少于指定百分比 如 10%;
    * 4.交易有效的最小金额， 少于指定值的交易不计入统计， 如 10000;
    * 6.交易间隔的有效时间， 设置追踪的有效时间， 如 2 两小时内发生的交易
    *
    * @param node
    * @param n
    * @return
    */
  def filterTransInfo(node: NodeInfo, n: NodeInfo): Boolean = {

    //时间条件过滤
    // 起始节点 > 目标节点   ||  起始节点 + 时间间隔 < 目标节点  过滤掉
    if (node.dealTime(0) > n.dealTime(0) || node.dealTime(0) + limit.getMillisecond() < n.dealTime(0)){
      return true
    }

    //组合中单笔金额不能少于指定百分比
    // 起始节点 * 百分比 > 目标节点  过滤掉
    if (node.totalMoney * limit.getSingleScale > n.totalMoney) {
      return true
    }

    //单笔交易不能少于指定金额
    // 目标节点 < 最小金额    过滤掉
    if (n.totalMoney < limit.getMinMoney) {
      return true
    }

    false
  }

  /**
    * 相同层级的数据对交易目标相同的账号进行合并
    *
    * @param nodeList
    * @param node
    * @return
    **/
  def unionTransInfo(nodeList: ListBuffer[NodeInfo], node: NodeInfo, queue: mutable.Queue[NodeInfo]): Boolean = {

    for (qNode <- queue) {
      //此处有个问题，针对组合的情况，如果把部分结果直接和队列进行合并结果数据不对。应该分情况处理，针对有组合的情况，组合确认后再合并
      //此层级的节点，与先插入到此层级的列表进行比较，如果目标相同对数据进行合并；
      if (qNode.toCardId.equals(node.toCardId)) {
        qNode.totalMoney = qNode.totalMoney + node.totalMoney
        qNode.cardId+=(node.cardId(0))
        qNode.dealMoney+=(node.dealMoney(0))
        qNode.dealTime+=(node.dealTime(0))
        return true
      }
    }
    false
    //nodeList+=node
  }


  /**
    *
    * 查询下游交易记录，并对交易数据进行过滤
    *
    * @param nodeInfo
    * @param layer
    * @param queue
    * @param endTransList
    * @param allPath
    * @return
    */

  def getTransInfo(nodeInfo: NodeInfo, layer: Int,
                   queue: mutable.Queue[NodeInfo],
                   endTransList: ListBuffer[NodeInfo],
                   allPath: Map[Long, Set[(Long, (Long, Long, Double))]]): ListBuffer[NodeInfo]={

    val nodeList = ListBuffer[NodeInfo]()


    val node = allPath.getOrElse(nodeInfo.toCardId, Nil)

    //println("节点【"+ nodeInfo.toCardId +"】的交易信息："+node)

    for(iter <- node.iterator){
      //println("iterator："+ iter)
      val n = NodeInfo(layer, iter._1, iter._2._1, iter._2._2, iter._2._3)
      //println("n："+ n.toString)
      //println("node:"+nodeInfo.toString)
      val r = filterTransInfo(nodeInfo, n)
      if (r == false){

        //对交易目标相同的账号进行合并
        //unionTransInfo(nodeList, n, queue)
        nodeList+=n
      }
    }

    //此节点没有后续交易，属于尾节点，插入到尾部列表，后续用于跨层级的合并
    if(nodeList.size==0) endTransList.append(nodeInfo)

    //println("nodeList="+nodeList)

    nodeList
  }


  /**
    * 从起始账号的所有交易数据中查找满足指定金额的所有组合数据， 返回所有组合信息
    * 1.通过配置信息限制组合深度
    * 2.通过配置信息限制交易金额的范围
    * 3.将所有符合交易金额范围的结果都获取到
    *
    * @param candidates   交易列表
    * @param target       目标金额
    *
    */
  def findCombineTrans( candidates:ListBuffer[NodeInfo], target:Double) :List[List[Int]] ={

    val allCombine = ListBuffer[List[Int]]()
    var combine = ListBuffer[Int]()

    def backtracking(sum: Double, startIndex: Int): Unit = {

      //if(allCombine.size>0) return

      if (sum > target * limit.getMinScale() && sum < target * limit.getMaxScale()) {
        allCombine.append(combine.toList)
        return
      }

      //1.超出限制总额，停止向树枝寻找，进行剪枝  2.超出指定组合笔数就跳过，进行剪枝
      for (i <- startIndex until candidates.size  if (sum + candidates(i).totalMoney <= target * limit.getMaxScale()) &&  combine.size < limit.getCombineNum()) {

        combine.append(i);

        // 从树枝节点继续往下搜索
        backtracking( sum + candidates(i).getTotalMoney(), i + 1);

        combine = combine.take(combine.size - 1)
      }
    }

    backtracking(0, 0)
    allCombine.toList
  }
}

object TransMain{
  def apply(): TransMain = new TransMain()
}

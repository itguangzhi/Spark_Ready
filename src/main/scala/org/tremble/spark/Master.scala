package org.tremble.spark

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Created by UCS-TREMBLE on 2019-04-16.
  */
class Master(val host:String,val port:Int) extends Actor{

  println("创建构造器了")
  // workerid -》 workerINFO
  val IdToWorker = new mutable.HashMap[String,WorkerInfo]()
  // workerinfo -> set 为了排序
  val workers = new mutable.HashSet[WorkerInfo]()
//  用于 接受消息
  override def receive = {

    case RegisterWorker(id,memory,cores) =>{
      //判断是否已经注册过了
      if (!IdToWorker.contains(id)){
        // 把worker的信息存储下来，保存到内存
        val workerInfo = new WorkerInfo(id,memory,cores)
        IdToWorker(id) = workerInfo
        workers += workerInfo
        sender!RegisteredMaster(s"akka.tcp://Master_System@$host:$port/user/MasterActer")
      }
    }
    case HeartBeat(workerId) =>{
      if(IdToWorker.contains(workerId)){
        val workerinfo = IdToWorker(workerId)
        //报告worker还活着
        val currentTime = System.currentTimeMillis()
        workerinfo.lastHeatBeatTime = currentTime
      }

    }

    case CheckTimeOutWorker =>{
      val currentTime = System.currentTimeMillis()
      val toRemove = workers.filter(x=>currentTime - x.lastHeatBeatTime > CHECK_INTERVAL)
      for (w<-toRemove){
        workers -= w
        IdToWorker -= w.id
      }
      println(workers.size)
    }
  }

//  超时检测间隔
  val CHECK_INTERVAL = 15000
  override def preStart(): Unit = {
    println("preStart 开始试用")
//    导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0.millis,CHECK_INTERVAL.millis,self,CheckTimeOutWorker)
  }


}
//半生对象
object Master{
  def main(args: Array[String]): Unit = {
    // actor的老大，辅助创建和监控下面的actor，他是单例的
    val host = args(0)
    val port = args(1).toInt
    // 准备配置，用于解析字符串的，可以创很多的Keyvalue
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    // 加载连接的配置文件
    val config = ConfigFactory.parseString(configStr)
    // 初始化akka的master 系统
    val actorSystem = ActorSystem("Master_System",config)

    // 创建actor
    val master = actorSystem.actorOf(Props(new Master(host,port)),"MasterActer")

    master ! "hello"
    actorSystem.awaitTermination()

  }
}
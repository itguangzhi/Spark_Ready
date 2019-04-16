package org.tremble.spark

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Created by UCS-TREMBLE on 2019-04-16.
  */
class Worker(val MasterHost:String,val MasterPort:Int) extends Actor{

  var worker_cli:ActorSelection = _
  // 在这里建立建立连接，（查找具体的某个actor）
  override def preStart(): Unit = {
    // 向master先建立连接，拿到代理对象，向master发送消息，从master向worker反馈消息
    // 建立TCP交互协议链接,链接在Master启动时打印
    worker_cli = context.actorSelection(s"akka.tcp://Master_System@$MasterHost:$MasterPort/user/MasterActer")
    //worker向master发送消息
    worker_cli!"connect"

  }

  override def receive = {
    case "replay" =>{
      println("replay from Master")
    }
    case "connected" =>{
      println("Master 已经链接上，并且给你回了一个消息")
    }
  }
}

object Worker{
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val MasterHost = args(2)
    val MasterPort = args(3).toInt
    // 准备配置，用于解析字符串的，可以创很多的Keyvalue（最简配置）
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    // 加载连接的配置文件
    val config = ConfigFactory.parseString(configStr)
    // 初始化akka的master 系统
    val actorSystem = ActorSystem("Worker_System",config)
    actorSystem.actorOf(Props(new Worker(MasterHost,MasterPort)),"WorkerActor")
    actorSystem.awaitTermination()

  }


}
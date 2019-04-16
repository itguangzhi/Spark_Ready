package org.tremble.spark

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Created by UCS-TREMBLE on 2019-04-16.
  */
class Master extends Actor{

  println("创建构造器了")
//  用于 接受消息
  override def receive = {

    case "connect" =>{
      println("***** 有一个客户端连接上了master")
      // 向worker返回消息
      sender!"connected"
    }
    case "hello" =>{
      println("hello 消息已经接收到了")

    }
  }

  override def preStart(): Unit = {
    println("preStart 开始试用")
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
    val master = actorSystem.actorOf(Props(new Master),"MasterActer")

    master ! "hello"
    actorSystem.awaitTermination()

  }
}
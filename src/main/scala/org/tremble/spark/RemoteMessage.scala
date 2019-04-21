package org.tremble.spark

/**
  * Created by UCS-TREMBLE on 2019-04-19.
  */

//消息要实现序列化
trait RemoteMessage extends Serializable

// worker 到master的信息
case class RegisterWorker(id : String, //id信息
                          memory : Int, // 内存信息
                          cores : Int //计算机CPU核的信息
                         ) extends RemoteMessage

// 从master到worker的信息
case class RegisteredMaster(masterURL:String,
                         ) extends RemoteMessage

// worker自己给自己发的消息
case object SendHeratBeat

case class HeartBeat(workerId :String ) extends RemoteMessage

//master自己发给自己的
case object CheckTimeOutWorker
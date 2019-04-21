package org.tremble.spark

/**
  * Created by UCS-TREMBLE on 2019-04-19.
  */
class WorkerInfo(val id:String,val memory:Int,val cores:Int) {
  //TODO 上一次心跳
  var lastHeatBeatTime:Long = _

}

package com.lyf.gmall.realtime.beans

case class AlterInfo(
                      mid:String,
                      uids:java.util.HashSet[String],
                      itemIds:java.util.HashSet[String],
                      events:java.util.List[String],
                      var ts:Long
                    ){
}

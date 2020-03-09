package com.lyf.gmall.realtime.beans

case class OrderInfo (id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String,
                      var create_hour: String,
                      var if_first_order:String){

  //敏感信息脱敏：eg tel (18823008899)
  this.consignee_tel = consignee_tel.take(3)+"****"+consignee_tel.takeRight(4)
  //截取订单创建日期和具体小时
  private val dateArr: Array[String] = create_time.split(" ")
  this.create_date = dateArr(0)
  this.create_hour = dateArr(1).split(":")(0)

}

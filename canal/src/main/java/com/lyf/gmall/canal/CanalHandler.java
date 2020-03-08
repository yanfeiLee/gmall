package com.lyf.gmall.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.lyf.gmall.canal.util.MyKafkaSender;
import com.lyf.gmall.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    //操作的数据行list
    List<CanalEntry.RowData> rowDatasList;
    //操作类型
    CanalEntry.EventType eventType;
    //操作的表
    String tableName;

    public CanalHandler(List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType, String tableName) {
        this.rowDatasList = rowDatasList;
        this.eventType = eventType;
        this.tableName = tableName;
    }

    public void handle(){
        if(this.tableName.equals("order_info")){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                //过滤掉 after无值的sql 操作，eg:delete
                if(afterColumnsList.size() !=0){
                    JSONObject jsonObject = new JSONObject();
                    //输出数据到控制台
                    for (CanalEntry.Column column : afterColumnsList) {
                        System.out.print(column.getName()+"=>"+column.getValue()+"|");
                        jsonObject.put(column.getName(), column.getValue());
                    }
                    System.out.println("------------------------------");
                    //组装数据发送到kafka
                    MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
                }
            }
        }
    }
}

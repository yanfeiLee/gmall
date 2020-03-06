package com.lyf.gmall.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class MyCanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop104", 11111), "example", "", "");
        //监空canalServer变化
        while (true){
            canalConnector.connect();
            //订阅监控的数据库
            canalConnector.subscribe("gmall.*");
            //获取指定sql个数的数据
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if(entries.size() == 0){
                System.out.println("server无变化。。。");
                //没有变化产生
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : entries) {
                    //判断业务类型，即进行事务开启，提交操作还是CRUD操作
                    if(entry.getEntryType() == CanalEntry.EntryType.ROWDATA){
                        //rowData数据
                        ByteString storeValue = entry.getStoreValue();
                        try {
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //操作的数据行list
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //操作类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //操作的表
                            String tableName = entry.getHeader().getTableName();


                            //根据具体业务处理数据
                            CanalHandler canalHandler = new CanalHandler(rowDatasList, eventType, tableName);
                            if(rowDatasList.size() != 0){
                                canalHandler.handle();
                            }

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                            new RuntimeException("反序列化异常");
                        }
                    }
                }
            }

        }
    }
}

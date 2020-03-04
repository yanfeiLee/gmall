package com.lyf.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.lyf.gmall.publisher.service.DauService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

@RestController
public class DauController {

    @Autowired
    DauService dauService;

    @GetMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){
        Long dauTotal = dauService.getDauTotal(date);
        //转换返回数据格式,为接口规定的格式
        ArrayList<Map> list = new ArrayList<>();
        HashMap mp1 = new HashMap();
        mp1.put("id", "dau");
        mp1.put("name", "新增日活");
        mp1.put("value", dauTotal);
        HashMap mp2 = new HashMap();
        mp2.put("id", "new_mid");
        mp2.put("name", "新增设备");
        mp2.put("value", 223);
        list.add(mp1);
        list.add(mp2);
        String jsonString = JSON.toJSONString(list);

        return jsonString;
    }

    @GetMapping("realtime-hour")
    public String getDauHour(@RequestParam("date")String date,@RequestParam("id") String id){

        HashMap resMap = new HashMap<String,Map>();
        if(id.equals("dau")){
            Map today = dauService.getDauHour(date);
            //添加今日数据
            resMap.put("today", today);

            //添加昨日数据
            Map yesterday = dauService.getDauHour(getYesterday(date));
            resMap.put("yesterday", yesterday);
        }

        String jsonString = JSON.toJSONString(resMap);
        return jsonString;
    }

    private String getYesterday(String date){
        //利用LocalDate 转换日期获取前一天
       /* DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse(date, df).minusDays(1);
        return localDate.toString();*/

       //利用apache.common包中的工具类获取前一天日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String res = "";
        try {
            Date td = sdf.parse(date);
            Date ystd = DateUtils.addDays(td, -1);
            res = sdf.format(ystd);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return res;
    }
}

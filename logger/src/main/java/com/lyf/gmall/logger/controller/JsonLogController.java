package com.lyf.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lyf.gmall.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController  //ResponseBody+Controller
public class JsonLogController {


    @Autowired
    KafkaTemplate kafkaTemplate;

    //    @RequestMapping(value = "log",method = RequestMethod.POST)
    @PostMapping("log")
    public String logHandler(@RequestParam("logString") String logString) {
        System.out.println(logString);
        //对日志添加服务器时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        String jsonString = jsonObject.toJSONString();

        //日志输出到文件和console
        log.info(jsonString);

        //按日志类型分别写入到kafka不同topic
        if (jsonObject.get("type").equals("startup")) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }
        return "success";
    }
}

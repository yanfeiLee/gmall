package com.lyf.gmall.publisher.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ESDirctionaryController {

    /**
    * @todo 返回es需要的字典库文件
    * @date 20/03/10 13:18
    * @param
    * @return
    *
    */
    @GetMapping("dictionary")
    public String getDictionary(){

        String res = "蓝廋香菇\r\n";
        return res;
    }
}

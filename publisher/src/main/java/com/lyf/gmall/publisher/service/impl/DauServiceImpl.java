package com.lyf.gmall.publisher.service.impl;

import com.lyf.gmall.publisher.mapper.DauMapper;
import com.lyf.gmall.publisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauServiceImpl implements DauService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        Long dauTotal = dauMapper.selectDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> maps = dauMapper.selectDauHour(date);
        //[(loghour:23,ct:22),(loghour:21,ct:221)]
        HashMap hm = new HashMap();
        for (Map map : maps) {
            hm.put(map.get("LOGHOUR"), map.get("CT"));
        }
        return hm;
    }


}

package com.lyf.gmall.publisher.service.impl;

import com.lyf.gmall.publisher.mapper.OrderMapper;
import com.lyf.gmall.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Double getOrderTotal(String date) {
        Double res = orderMapper.selectOrderTotal(date);
        return res == null ? 0.0 : res;
    }

    @Override
    public Map getOrderHour(String date) {
        List<Map> maps = orderMapper.selectOrderHour(date);
        HashMap<Object, Object> res = new HashMap<>();
        for (Map map : maps) {
            res.put(map.get("CREATE_HOUR"), map.get("TAMT"));
        }
        return res;
    }
}

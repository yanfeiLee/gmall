package com.lyf.gmall.publisher.mapper;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface OrderMapper {
    //获取当日总订单数据
    public Double selectOrderTotal(String date);

    //获取当日分时订单数据
    public List<Map> selectOrderHour(String date);
}

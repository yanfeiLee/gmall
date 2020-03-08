package com.lyf.gmall.publisher.service;

import java.util.Map;

public interface OrderService {

    public Double getOrderTotal(String date);

    public Map getOrderHour(String date);

}

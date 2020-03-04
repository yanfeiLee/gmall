package com.lyf.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

public interface DauService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}

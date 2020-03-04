package com.lyf.gmall.publisher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface DauMapper {
    public Long selectDauTotal(String date);

    public List<Map> selectDauHour(String date);
}

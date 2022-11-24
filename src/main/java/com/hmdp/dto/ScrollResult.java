package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;
    //上次查询的最小时间
    private Long minTime;
    //偏移量
    private Integer offset;
}

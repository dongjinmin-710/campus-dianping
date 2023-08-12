package com.hmdp.dto;

import lombok.Data;
import java.util.List;

/**
 * 关注FEED流 推送返回对象类
 */
@Data
public class ScrollResult {
    /**
     * 推文
     */
    private List<?> list;

    /**
     * 推文中最小的时间戳
     */
    private Long minTime;

    /**
     * 偏移量
     */
    private Integer offset;
}

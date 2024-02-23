package com.stmq.storage.pool.allocator;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Author: Stan sai
 * Date: 2024/2/22 05:16
 * description:
 */
@Getter
@AllArgsConstructor
public enum Size {
    HUGE(3, null),   //32KB
    LARGE(2, HUGE),     //4KB
    NORMAL(1, LARGE),    //512B
    SMALL(0, NORMAL),    //64B
    ;
    final int code;
    final Size next;
}

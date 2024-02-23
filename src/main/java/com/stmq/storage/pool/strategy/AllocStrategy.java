package com.stmq.storage.pool.strategy;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Author: Stan sai
 * Date: 2024/2/22 05:21
 * description:
 */
@AllArgsConstructor
@Getter
public enum AllocStrategy {
    //每个区相同大小
    BALANCE(()->new int[]{2, 2, 2, 2}),
    ;

    final Strategy strategy;
}

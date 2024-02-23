package com.stmq.storage.pool.strategy;

/**
 * Author: Stan sai
 * Date: 2024/2/22 05:24
 * description:
 * 分配每个size的page内存比例
 *  返回 4 长度的数组, 相加必须为8的倍数
 */
public interface Strategy {
    int[] ratios();
}

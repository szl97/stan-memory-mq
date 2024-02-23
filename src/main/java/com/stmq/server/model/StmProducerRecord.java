package com.stmq.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:08
 * description:
 */
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StmProducerRecord<T> implements Serializable {
    private static final long serialVersionUID = 7373984872572414699L;
    String key;
    T data;
}

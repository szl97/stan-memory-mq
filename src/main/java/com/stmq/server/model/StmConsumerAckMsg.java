package com.stmq.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Author: Stan sai
 * Date: 2024/2/22 20:08
 * description:
 */
@Builder
@AllArgsConstructor
public class StmConsumerAckMsg {
    @Getter
    String id;
}

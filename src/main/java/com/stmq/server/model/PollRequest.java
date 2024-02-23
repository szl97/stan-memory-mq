package com.stmq.server.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:16
 * description:
 */
@Getter
@AllArgsConstructor
public class PollRequest {
    String topic;
    int milliSeconds;
}

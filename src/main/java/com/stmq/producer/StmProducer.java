package com.stmq.producer;

import com.stmq.server.manager.StmBroker;
import com.stmq.server.model.StmProducerRecord;

import java.io.IOException;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:41
 * description:
 */
public class StmProducer {
    final StmBroker broker;

    public StmProducer(StmBroker broker) {
        this.broker = broker;
    }

    public <T> boolean send(String key, T data) throws IOException {
        return send(key, data, 5);
    }

    public <T>  boolean send(String key, T data, int retryTimes) throws IOException {
        if(retryTimes == 0) {
            return false;
        }
        StmProducerRecord<T> record = new StmProducerRecord<>(key, data);
        if(!broker.receive(record).isSucceed()) {
           return send(key, data, --retryTimes);
        }
        return true;
    }
}

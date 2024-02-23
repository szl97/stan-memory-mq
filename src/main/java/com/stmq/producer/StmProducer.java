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
        return send(key, data, null);
    }

    public <T> boolean send(String key, T data, Runnable callback) throws IOException {
        return send(key, data, 5, callback);
    }

    public <T>  boolean send(String key, T data, int retryTimes, Runnable callback) throws IOException {
        if(retryTimes == 0) {
            if(callback == null) {
                throw new RuntimeException("send msg failed");
            } else {
                callback.run();
            }
        }
        StmProducerRecord<T> record = new StmProducerRecord<>(key, data);
        if(!broker.receive(record).isSucceed()) {
           return send(key, data, --retryTimes, callback);
        }
        return true;
    }
}

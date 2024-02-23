package com.stmq.consumer;

import com.stmq.server.manager.StmBroker;
import com.stmq.server.model.PollRequest;
import com.stmq.server.model.StmConsumerAckMsg;
import com.stmq.server.model.StmConsumerRecord;

import java.util.Arrays;
import java.util.List;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:41
 * description:
 */
public abstract class StmConsumer {
    final StmBroker broker;
    final List<String> topics;

    public StmConsumer(StmBroker broker, String... topics) throws Exception {
        if(topics == null) {
            throw new Exception("consumer muset subscribe one topic at least");
        }
        this.broker = broker;
        this.topics = Arrays.stream(topics).toList();
        init();
    }

    private void init() {
        for(String topic : topics) {
            subscribe(topic);
        }
    }

    public abstract  <T> void consumer(StmConsumerRecord<T> record);

    private <T> void subscribe(String topic) {
        new Thread(()->{
            while (true) {
                StmConsumerRecord<T> record = null;
                try {
                    record = broker.pollFormBroker(new PollRequest(topic, 500));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if(record != null) {
                    consumer(record);
                    broker.receiveAck(new StmConsumerAckMsg(record.getKey()));
                }
            }
        }).start();
    }
}

package com.stmq.server.manager;

import com.stmq.server.model.*;
import com.stmq.server.queue.StmQueue;
import com.stmq.storage.byteBuf.ByteBuf;
import com.stmq.storage.pool.allocator.PooledAllocator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:05
 * description:
 */
public class StmBroker {
    static Logger logger = Logger.getLogger(StmBroker.class);
    private final ConcurrentHashMap<String, StmQueue> topicMap;
    final ConcurrentHashMap<String, Boolean> lockMap;
    final int maxPooledPages;
    final ScheduledThreadPoolExecutor executor;
    final int ackTimeOut;
    final ConcurrentHashMap<String, AckData> ackMap;
    @Setter
    int minQueueSize;
    final int defaultWaitMillis;
    @Getter
    static volatile StmBroker BROKER;
    public static StmBroker start(int... args) throws Exception {
        if(args == null) {
            PooledAllocator.initializeSinglet();
        } else {
            PooledAllocator.initializeSinglet(args[0], args.length == 1 ? args[0] : args[1]);
        }
        if(BROKER == null) {
            synchronized (StmBroker.class) {
                if(BROKER == null) {
                    BROKER = new StmBroker();
                }
            }
        }
        return BROKER;
    }
    private StmBroker() {
        topicMap = new ConcurrentHashMap<>();
        lockMap = new ConcurrentHashMap<>();
        maxPooledPages = ByteBuf.getMaxPooledBuf();
        executor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() + 1,
                r->new Thread("StmBroker-executor"));
        ackTimeOut = 10;
        ackMap = new ConcurrentHashMap<>();
        minQueueSize = 1024;
        defaultWaitMillis = 500;
    }
    public <T> StmServerAckMsg receive(StmProducerRecord<T> record) throws IOException {
        if(topicMap.containsKey(record.getKey())) {
            StmQueue queue = topicMap.get(record.getKey());
            ByteBuf byteBuf = ByteBuf.writeObjet(record.getData());
            boolean result = queue.offer(byteBuf);
            if(!result) {
                byteBuf.release();
            }
            return StmServerAckMsg.builder().succeed(result).build();
        } else {
            createTopic(record.getKey());
            return receive(record);
        }
    }
    public StmServerAckMsg receiveAck(StmConsumerAckMsg ackMsg) {
        logger.debug("release msg " + ackMsg.getId());
        AckData ackData = ackMap.remove(ackMsg.getId());
        if(ackData != null) {
           return StmServerAckMsg.builder().succeed(ackData.data.release()).build();
        }
        return StmServerAckMsg.builder().succeed(false).build();
    }

    public <T> StmConsumerRecord<T> pollFormBroker(PollRequest pollRequest) throws InterruptedException, IOException, ClassNotFoundException {
        String key = pollRequest.getTopic();
        if(!topicMap.containsKey(key)) {
            createTopic(key);
        }
        StmQueue stmQueue = topicMap.get(key);
        int millis = pollRequest.getMilliSeconds() < 0 ? defaultWaitMillis : pollRequest.getMilliSeconds();
        ByteBuf byteBuf = stmQueue.tryTake(millis);
        if(byteBuf == null) {
            return null;
        } else {
            T object = byteBuf.readObject();
            String uuid = UUID.randomUUID().toString();
            StmConsumerRecord<T> record = new StmConsumerRecord<>(uuid, object);
            ackMap.put(record.getKey(), new AckData(key, byteBuf));
            executor.schedule(()->{
                ackMap.remove(key);
                topicMap.get(key).offer(byteBuf);
            }, ackTimeOut, TimeUnit.SECONDS);
            return record;
        }
    }

    private void createTopic(String key) {
        while (lockMap.putIfAbsent(key, true) != null) {
        }
        if(topicMap.containsKey(key)) {
            lockMap.remove(key);
        } else {
            StmQueue stmQueue = new StmQueue(key, Math.max(maxPooledPages/512, minQueueSize), Math.max(maxPooledPages, minQueueSize));
            topicMap.put(key, stmQueue);
            lockMap.remove(key);
            logger.debug("create topic " + key);
        }
    }

    @AllArgsConstructor
    @Getter
    static class AckData {
        String topic;
        ByteBuf data;
    }

}

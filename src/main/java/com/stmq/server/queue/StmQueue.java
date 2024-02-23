package com.stmq.server.queue;

import com.stmq.storage.byteBuf.ByteBuf;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Author: Stan sai
 * Date: 2024/2/22 20:23
 * description:
 * 数组+锁，实现阻塞队列
 * 读、写时只加读锁，不会阻塞
 * 扩容时加写锁，尽量通过配置足够长度来避免扩容
 * 阻塞的poll方法使用sleep和interrupt,使用了线程安全的ConcurrentLinkedQueue存放timed_waiting的消费者线程
 */
public class StmQueue {
    static Logger logger = Logger.getLogger(StmQueue.class);
    final String topic;
    volatile ByteBuf[] data;
    final int maxCapacity;
    volatile int capacity;
    final int up;
    AtomicInteger offset = new AtomicInteger(0);
    AtomicInteger position = new AtomicInteger(0);
    final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();
    final ConcurrentLinkedQueue<Thread> deque = new ConcurrentLinkedQueue<>();

    public StmQueue(String topic, int capacity, int maxCapacity) {
        this.topic = topic;
        this.maxCapacity = maxCapacity;
        this.capacity = capacity;
        up = capacity;
        data = new ByteBuf[capacity];

    }

    public boolean offer(ByteBuf byteBuf) {
        int index = acquireWriteIndex();
        if(index == -1) {
            return false;
        }
        data[index] = byteBuf;
        Thread thread = deque.poll();
        if(thread != null) {
            thread.interrupt();
        }
        return true;
    }

    public ByteBuf take() {
        while (data[position.get()] == null) {
        }
        int cur = position.get();
        int next = cur == capacity - 1 ? 0 : cur + 1;
        ByteBuf byteBuf = data[cur];
        LOCK.readLock().lock();
        if(byteBuf == null || !position.compareAndSet(cur, next)) {
            LOCK.readLock().unlock();
            return take();
        }
        data[cur] = null;
        LOCK.readLock().unlock();
        return byteBuf;
    }

    public ByteBuf tryTake() {
        if (data[position.get()] == null) {
            return null;
        }
        int cur = position.get();
        int next = cur == capacity - 1 ? 0 : cur + 1;
        ByteBuf byteBuf = data[cur];
        LOCK.readLock().lock();
        if(byteBuf == null || !position.compareAndSet(cur, next)) {
            LOCK.readLock().unlock();
            return tryTake();
        }
        data[cur] = null;
        LOCK.readLock().unlock();
        return byteBuf;
    }

    public ByteBuf tryTake(int millis) {
        if (data[position.get()] == null) {
            if(millis == 0) {
                return null;
            }
            Thread thread = Thread.currentThread();
            deque.offer(thread);
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.interrupted();
                return tryTake();
            }
            return null;
        }
        int cur = position.get();
        int next = cur == capacity - 1 ? 0 : cur + 1;
        ByteBuf byteBuf = data[cur];
        LOCK.readLock().lock();
        if(byteBuf == null || !position.compareAndSet(cur, next)) {
            LOCK.readLock().unlock();
            return tryTake();
        }
        data[cur] = null;
        LOCK.readLock().unlock();
        return byteBuf;
    }

    /*
        offsets代表待写入位置，有数据代表数组满了。

        扩容时：
        1、offset.set(position.get()+cap);
        2、data = byteBufs;
        3、capacity = data.length;

        这三步顺序不能变，否则线程不安全
        其中2，3改变位置可能出现数组边界溢出异常
        if(cur > capacity) {判断 1执行了，3没执行的情况
           return acquireWriteIndex();
        }

        if(data[cur] != null) {获取cur时1没执行，如果到这一步1还没执行，一定会命中
                               如果获取cur后，这一步前，1执行了，2也执行了，可能不会命中这个条件
            lock();
        }
        ...

        if(offset.compareAndSet(cur,next)) {如果获取cur后，判断data[cur] != null前，1执行了，2也执行了，这里一定命中
            return cur;
        }

     */
    private int acquireWriteIndex() {
        int cur = offset.get();
        if(cur > capacity) {
           return acquireWriteIndex();
        }
        int next = cur == capacity - 1 ? 0 : cur + 1;
        if(data[cur] != null) {
            logger.debug("扩容");
            logger.debug(LOCK.getReadHoldCount());
            logger.debug(LOCK.isWriteLocked());
            LOCK.writeLock().lock();
            if (data[cur] == null || offset.get() != cur) {
                LOCK.writeLock().unlock();
                return acquireWriteIndex();
            } else {
                boolean succeed = upCapacity();
                LOCK.writeLock().unlock();
                if(!succeed) {
                    return -1;
                }
                return acquireWriteIndex();
            }
        }
        LOCK.readLock().lock();
        if(offset.compareAndSet(cur,next)) {
            logger.debug("成功获取写入权： "+cur+" 位置");
            LOCK.readLock().unlock();
            return cur;
        }
        LOCK.readLock().unlock();
        return acquireWriteIndex();
    }

    private boolean upCapacity(){
        int cap = capacity;
        if(cap + up > maxCapacity) {
            return false;
        }
        ByteBuf[] byteBufs = new ByteBuf[capacity+up];
        for(int i = position.get(); i < position.get()+cap; i++) {
            int index = i;
            if(index >= cap) {
                index = index - cap;
            }
            byteBufs[i] = data[index];
        }
        offset.set(position.get()+cap);
        if(offset.get() == position.get() + cap) {//防止指令重排序，出现并发问题
            data = byteBufs;
            if(data.length > capacity) {
                capacity = data.length;
            }
        }
        logger.debug("扩容完成");
        log();
        return true;
    }

    public void log() {
        logger.debug("此时cap:"+capacity);
        logger.debug("此时offset:"+offset);
        logger.debug("此时position:"+position);
        int p = position.get();
        int l = offset.get();
        if(l == p) {
            return;
        }
        int total = l > p ? (l - p) : capacity - (p - l);
        for(int i = p; i < p+total; i++) {
            int index = i;
            if(index >= capacity) {
                index = index - capacity;
            }
            logger.info("接下来第"+(i-p+1)+"个读取：" + data[index].toString());
        }
    }
}

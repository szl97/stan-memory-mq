package com.stmq.storage.pool.allocator;

import com.stmq.storage.byteBuf.ByteBuf;
import com.stmq.storage.byteBuf.PooledByteBuf;
import com.stmq.storage.pool.strategy.AllocStrategy;
import com.stmq.storage.pool.strategy.Strategy;
import lombok.Getter;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author: Stan sai
 * Date: 2024/2/22 06:00
 * description: 内存池设计
 * 1、一次分配一页，便于管理回收，避免内存碎片
 * 四种page，s(64B per page), n(512B per page), l(4KB per page), h(32KB per page)
 * 2、依次向上扩容：s不够，找到一个n拆为8个; n不够，找到一个l拆为8个n; l不够，h -> 8l
 * h不够会申请新的h，直到达到最大内存。
 * page拆分后不支持恢复，因为合并时需要对数据段加锁，且不确定是否后续依然要拆分，频繁拆分+合并会影响性能
 * 3、使用堆外内存
 * 4、page用数组记录，记录近似的最后一次分配位置（避免锁竞争，线程不安全，只需要粗略记录，方便下次分配时寻址）
 * 思路：
 * 用于实现内存消息队列，消息队列用内存存储，场景必然是消费迅速的，考虑方向是尽量避免线程的锁竞争
 * 使用数组，每个线程去修改数组指定位置，是线程安全的。
 * 只有获取数组位置修改权时需要使用加锁，在这里，使用CAS，将used数组对应位置改为true，即代表内存被某个线程使用了。
 * 但是其缺陷是寻址问题，即找到可用page。
 * 如果使用一个线程安全的容器，将空page都放进容器即可。但是频繁读写下，性能瓶颈由该容器决定。
 * 使用数组从头开始分配，并记录粗略的最后一次分配位置，每次查找从最后一次的位置向后找，可以减少遍历次数
 * 如果找到数组末尾，从头开始寻址，因为消息队列的特性，前面的内存大概率是已经被消费者处理过，释放掉了
 * 如果内存不够，便会扩容，扩容时是线程阻塞的，每个区都是一次只申请8个page，扩容代价较大，需要遍历数组
 * 这个思路的主要缺陷就在于page不够时，需要遍历数组
 * 这需要根据业务数据量级和单条消息的内存大小，来选择合适的内存大小和page区分配策略
 * 如果配置合理，基于内存消息队列的特性和场景，除非消费者阻塞、挂了或者消费能力远小于发送者的发送频率，
 * 否则page应该不会不够。
 * 如果是消费者阻塞或者挂了，那么生产者写入慢，是可以容忍的，因为整个系统的吞吐量瓶颈已经由消费者端决定了
 * 如果是由于消费者消费能力不足，那就应该考虑增加消费者了
 */
public class PooledAllocator implements Allocator {
    static Logger logger = Logger.getLogger(PooledAllocator.class);
    int curSize;
    int maxSize;
    Strategy strategy;
    int[] nums;
    ByteBuffer[][] pages;
    AtomicBoolean[][] used;
    int[] indexes;
    final int[] pageSizes = new int[]{64, 512, 4096, 4096*8};
    @Getter
    int maxPages;
    ReentrantLock upLock = new ReentrantLock();

    static volatile PooledAllocator SINGLET;

    static final ReentrantLock SINGLET_LOCK = new ReentrantLock();

    public static PooledAllocator getInstance() {
        return SINGLET;
    }

    public static void initializeSinglet() throws Exception {
        initializeSinglet(512 * 1024 * 1024, 1024 * 1024 * 1024);
    }

    public static void initializeSinglet(int initSize, int maxSize) throws Exception {
        initializeSinglet(initSize, maxSize, AllocStrategy.BALANCE.getStrategy());
    }

    public static void initializeSinglet(int initSize, int maxSize, Strategy strategy) throws Exception {
        if(SINGLET != null) {
            return;
        }
        SINGLET_LOCK.lock();
        if(SINGLET == null) {
            SINGLET = new PooledAllocator(initSize, maxSize, strategy);
        }
        SINGLET_LOCK.unlock();
    }


    private PooledAllocator(int initSize, int maxSize, Strategy strategy) throws Exception {
        int[] ratios = strategy.ratios();
        if(ratios == null || ratios.length != 4 || (ratios[0] + ratios[1] + ratios[2] + ratios[3]) % 8 != 0) {
            throw new Exception("wrong partition strategy");
        }
        int sum = ratios[0] + ratios[1] + ratios[2] + ratios[3];
        this.curSize = initSize;
        this.maxSize = maxSize;
        this.strategy = strategy;
        nums = new int[4];
        nums[0] = ((initSize / sum) * ratios[0])/pageSizes[0];
        nums[1] = ((initSize / sum) * ratios[1])/pageSizes[1];
        nums[2] = ((initSize / sum) * ratios[2])/pageSizes[2];
        nums[3] = ((initSize / sum) * ratios[3])/pageSizes[3];
        maxPages = maxSize / pageSizes[0];
        ByteBuffer[] small = new ByteBuffer[maxPages];
        ByteBuffer[] normal = new ByteBuffer[maxPages/8];
        ByteBuffer[] large = new ByteBuffer[maxPages/64];
        ByteBuffer[] huge = new ByteBuffer[maxPages/512];
        pages = new ByteBuffer[4][];
        pages[0] = small;
        pages[1] = normal;
        pages[2] = large;
        pages[3] = huge;
        used = new AtomicBoolean[4][];
        used[0] = new AtomicBoolean[maxPages];
        used[1] = new AtomicBoolean[maxPages/8];
        used[2] = new AtomicBoolean[maxPages/64];
        used[3] = new AtomicBoolean[maxPages/512];
        indexes = new int[4];
        for(int i = 0; i < maxPages; i++) {
            if(i < maxPages/512) {
                used[3][i] = new AtomicBoolean(false);
            }
            if(i < maxPages/64) {
                used[2][i] = new AtomicBoolean(false);
            }
            if(i < maxPages/8) {
                used[1][i] = new AtomicBoolean(false);
            }
            used[0][i] = new AtomicBoolean(false);
        }
        Arrays.fill(indexes, 0);
        init();
    }

    private void init() {
        logger.info("初始化内存池大小:"+curSize);
        ByteBuffer total = ByteBuffer.allocateDirect(curSize);
        logger.info("初始化small page个数:"+nums[0]);
        sliceAndStore(total.position(0)
                .limit(nums[0]*pageSizes[0])
                .slice(), pageSizes[0], pages[0], 0);
        logger.info("初始化normal page个数:"+nums[1]);
        sliceAndStore(total.position(nums[0]*pageSizes[0])
                .limit(nums[0]*pageSizes[0]+nums[1]*pageSizes[1])
                .slice(), pageSizes[1], pages[1], 0);
        logger.info("初始化large page个数:"+nums[2]);
        sliceAndStore(total.position(nums[0]*pageSizes[0]+nums[1]*pageSizes[1])
                .limit(nums[0]*pageSizes[0]+nums[1]*pageSizes[1]+nums[2]*pageSizes[2])
                .slice(), pageSizes[2], pages[2], 0);
        logger.info("初始化huge page个数:"+nums[3]);
        sliceAndStore(total.position(nums[0]*pageSizes[0]+nums[1]*pageSizes[1]+nums[2]*pageSizes[2])
                .limit(nums[0]*pageSizes[0]+nums[1]*pageSizes[1]+nums[2]*pageSizes[2]+nums[3]*pageSizes[3])
                .slice(), pageSizes[3], pages[3], 0);
    }

    private void sliceAndStore(ByteBuffer buffer, int size, ByteBuffer[] buffers, int curIndex) {
        int pos, limit = 0;
        buffer.limit(limit);
        while (limit < buffer.capacity()) {
           pos = buffer.limit();
           limit = pos + size;
           buffers[curIndex++] = buffer.position(pos).limit(limit).slice();
        }
    }

    @Override
    public ByteBuf alloc(int size) {
        Size type = fetchSizeType(size);
        if(type == null) {
            return null;
        }
        int code = type.getCode();
        int index = acquireUsefulIndex(used[code], nums[code], indexes[code]);
        //扩容时DCL
        if(index == -1) {
            logger.debug("获取扩容锁");
            upLock.lock();
            logger.debug("获取扩容锁成功");
            index = acquireUsefulIndex(used[code], nums[code], indexes[code]);
            if (index == -1) {
                logger.debug(type.name()+" page不足，开始扩容");
                if (upPage(type)) {
                    upLock.unlock();
                    logger.debug("扩容成功释放锁");
                    return alloc(size);
                } else {
                    logger.debug("扩容失败返回null");
                    upLock.unlock();
                    return null;
                }
            }
            logger.debug("无需扩容,释放锁");
            upLock.unlock();
        }
        indexes[code] = index;
        logger.debug("成功分配第"+(index+1)+"个"+type.name()+" page,并记录此次查找的位置："+index);
        return PooledByteBuf.builder().type(type).index(index).allocator(this).build();
    }

    @Override
    public boolean release(ByteBuf byteBuf) {
        Size type = ((PooledByteBuf) byteBuf).getType();
        boolean b = used[type.getCode()][((PooledByteBuf) byteBuf).getIndex()].compareAndSet(true, false);
        if(b) {
            logger.debug("成功释放第"+(((PooledByteBuf) byteBuf).getIndex()+1)+"个"+type.name()+" page");
        } else {
            logger.debug("其他线程已经释放了第"+(((PooledByteBuf) byteBuf).getIndex()+1)+"个"+type.name()+" page");
        }
        byteBuf.destroy();
        return b;
    }

    @Override
    public ByteBuffer fetchBuffer(ByteBuf byteBuf) {
        Size type = ((PooledByteBuf) byteBuf).getType();
        return pages[type.getCode()][((PooledByteBuf) byteBuf).getIndex()];
    }


    private boolean upPage(Size type) {
        logger.debug(type.name()+" page扩容过程开始");
        if(type != Size.HUGE) {
            int code = type.getCode();
            logger.debug("开始寻找可用的"+type.getNext().name()+" page");
            int index = acquireUsefulIndex(used[code+1], nums[code+1], indexes[code+1]);
            if (index == -1) {
                logger.debug("找不到可用的"+type.getNext().name()+" page, 对该page的区域进行扩容");
                if(upPage(type.getNext())) {
                    logger.debug(type.getNext().name()+" page扩容成功，继续对"+type.name()+" page进行扩容");
                    return upPage(type);
                } else {
                    logger.debug(type.getNext().name()+" page扩容失败，池最大内存不足无法扩容");
                    return false;
                }
            }
            logger.debug("记录此次找到可用"+type.getNext().name()+" page的位置："+index);
            indexes[code+1] = index;
            logger.debug("第"+index+"个"+type.getNext().name()+" page开始分片");
            sliceAndStore(pages[code+1][index], pageSizes[code], pages[code], nums[code]);
            nums[code] += 8;
            logger.debug(type.name()+" page数量增加为："+nums[code]);
        } else {
            if(curSize == maxSize) {
                logger.debug("最大内存不足,扩容失败");
                return false;
            }
            int num = pageSizes[3] * 8 > maxSize - curSize ? (maxSize - curSize) / pageSizes[3] : 8;
            ByteBuffer hugePage = ByteBuffer.allocateDirect(pageSizes[3] * num);
            sliceAndStore(hugePage, pageSizes[3], pages[3], nums[3]);
            nums[3] += num;
            curSize += pageSizes[3] * num;
            logger.debug("hugePage此次扩容+"+num+"页"+"，此时数量为："+nums[3]);
        }
        return true;
    }

    private int acquireUsefulIndex(AtomicBoolean[] used, int curSize, int lastIndex) {
        if(curSize == 0) {
            logger.debug("未找到可用页");
            return -1;
        }
        int start = lastIndex;
        logger.debug(start+"位置开始遍历查找");
        while (lastIndex != curSize) {
            if(!used[lastIndex].get()) {
                if(used[lastIndex].compareAndSet(false, true)) {
                    logger.debug(lastIndex+"位置找到可用页");
                    return lastIndex;
                }
            }
            ++lastIndex;
        }
        return acquireUsefulIndex(used, start, 0);
    }

    private Size fetchSizeType(int size) {
        if(size <= pageSizes[0]) {
            return Size.SMALL;
        } else if(size <= pageSizes[1]) {
            return Size.NORMAL;
        } else if(size <= pageSizes[2]) {
            return Size.LARGE;
        } else if(size <= pageSizes[3]) {
            return Size.HUGE;
        }
        return null;
    }

    public void logInfo() {
        logger.debug("normal数量："+nums[1]);
        logger.debug("large数量："+nums[2]);
        int use = 0;
        for(int i = 0; i < nums[1]; i++) {
            if(!used[1][i].get()) {
                ++use;
            }
        }
        logger.debug("可用normal数量："+use);
        use = 0;
        for(int i = 0; i < nums[2]; i++) {
            if(!used[2][i].get()) {
                ++use;
            }
        }
        logger.debug("可用large数量："+use);
    }

}

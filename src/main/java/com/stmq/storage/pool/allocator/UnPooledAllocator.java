package com.stmq.storage.pool.allocator;

import com.stmq.storage.byteBuf.ByteBuf;
import com.stmq.storage.byteBuf.UnPooledByteBuf;
import lombok.Getter;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;


/**
 * Author: Stan sai
 * Date: 2024/2/22 18:57
 * description: 申请大于32kb的内存时存在内存池外
 */
public class UnPooledAllocator implements Allocator {
    static Logger logger = Logger.getLogger(UnPooledAllocator.class);
    private UnPooledAllocator() {

    }
    @Getter
    static final UnPooledAllocator INSTANCE = new UnPooledAllocator();

    @Override
    public ByteBuf alloc(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        logger.debug("申请非池化内存"+size+"Byte");
        return UnPooledByteBuf.builder().buffer(buffer).build();
    }

    @Override
    public boolean release(ByteBuf byteBuf) {
        return true;
    }

    @Override
    public ByteBuffer fetchBuffer(ByteBuf byteBuf) {
        return null;
    }

}

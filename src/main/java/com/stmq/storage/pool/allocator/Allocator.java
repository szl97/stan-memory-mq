package com.stmq.storage.pool.allocator;

import com.stmq.storage.byteBuf.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Author: Stan sai
 * Date: 2024/2/22 18:16
 * description:
 */
public interface Allocator {
    static Allocator getInstance(int size) {
        return size > 4096 * 8 ? UnPooledAllocator.getINSTANCE() : PooledAllocator.getInstance();
    }
    ByteBuf alloc(int size);

    boolean release(ByteBuf byteBuf);

    ByteBuffer fetchBuffer(ByteBuf byteBuf);
}

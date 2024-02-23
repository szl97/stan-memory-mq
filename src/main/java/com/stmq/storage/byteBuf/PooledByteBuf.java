package com.stmq.storage.byteBuf;


import com.stmq.storage.pool.allocator.PooledAllocator;
import com.stmq.storage.pool.allocator.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

/**
 * Author: Stan sai
 * Date: 2024/2/22 05:11
 * description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class PooledByteBuf extends ByteBuf {
    Size type;
    int index;
    PooledAllocator allocator;

    @Override
    public boolean release() {
        getBuffer().clear();
        if(allocator == null) {
            return false;
        }
        return allocator.release(this);
    }

    @Override
    public ByteBuffer getBuffer() {
        if(allocator == null) {
            return null;
        }
        return allocator.fetchBuffer(this);
    }

    @Override
    public void destroy() {
        allocator = null;
    }

    @Override
    public String toString() {
        return type.name() + " page " + index + "位置";
    }
}

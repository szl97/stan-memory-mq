package com.stmq.storage.byteBuf;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * Author: Stan sai
 * Date: 2024/2/22 19:16
 * description:
 */
@AllArgsConstructor
@Getter
@Builder
public class UnPooledByteBuf extends ByteBuf {
    ByteBuffer buffer;

    @Override
    public boolean release() {
        buffer = null;
        return true;
    }

    @Override
    public void destroy() {

    }

    @Override
    public String toString() {
        return "UnPooled";
    }
}

package com.stmq.storage.byteBuf;

import com.stmq.storage.pool.allocator.Allocator;
import com.stmq.storage.pool.allocator.PooledAllocator;
import com.stmq.storage.pool.allocator.UnPooledAllocator;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Author: Stan sai
 * Date: 2024/2/22 05:12
 * description:
 */
public abstract class ByteBuf {
   public static int getMaxPooledBuf() {
      return PooledAllocator.getInstance().getMaxPages();
   }
   public static ByteBuf writeObjet(Object o) throws IOException {
      byte[] bytes;
      try(ByteArrayOutputStream bs = new ByteArrayOutputStream();
          ObjectOutputStream os = new ObjectOutputStream(bs)) {
         os.writeObject(o);
         bytes = bs.toByteArray();
      }
      ByteBuf byteBuf = alloc(bytes.length, Allocator.getInstance(bytes.length));
      if(byteBuf == null) {
         byteBuf = alloc(bytes.length, UnPooledAllocator.getINSTANCE());
      }
      ByteBuffer buffer = byteBuf.getBuffer();
      buffer.put(bytes);
      buffer.flip();
      return byteBuf;
   }
   public <T> T readObject() throws IOException, ClassNotFoundException {
      ByteBuffer buffer = getBuffer();
      buffer.mark();
      byte[] bytes = new byte[buffer.limit() - buffer.position()];
      buffer.get(bytes);
      T object;
      try(ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
          ObjectInputStream os = new ObjectInputStream(bs)) {
         object = (T) os.readObject();
      }
      buffer.reset();
      return object;
   }

   public <T> T readObjectAndRelease() throws IOException, ClassNotFoundException {
      T object = readObject();
      release();
      return object;
   }

   private static ByteBuf alloc(int size, Allocator allocator) {
       return allocator.alloc(size);
   }

   public abstract boolean release();
   public abstract ByteBuffer getBuffer();

   public abstract void destroy();
}

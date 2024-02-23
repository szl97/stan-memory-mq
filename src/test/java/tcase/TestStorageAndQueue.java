package tcase;

import com.stmq.server.queue.StmQueue;
import com.stmq.storage.byteBuf.ByteBuf;
import com.stmq.storage.byteBuf.PooledByteBuf;
import com.stmq.storage.byteBuf.UnPooledByteBuf;
import com.stmq.storage.pool.allocator.PooledAllocator;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Author: Stan sai
 * Date: 2024/2/22 20:05
 * description:
 */
public class TestStorageAndQueue {
    static Logger logger = Logger.getLogger(TestStorageAndQueue.class);
    static {
        try {
            PooledAllocator.initializeSinglet(128 * 1024, 256 * 1024);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void checkSizeForRecordAndStudent() throws IOException {
        Record record = new Record();
        try(ByteArrayOutputStream bs = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bs)) {
            os.writeObject(record);
            byte[] bytes = bs.toByteArray();
            logger.debug("record大小为"+bytes.length);
        }
        try(ByteArrayOutputStream bs = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bs)) {
            os.writeObject(record.student);
            byte[] bytes = bs.toByteArray();
            logger.debug("student大小为"+bytes.length);
        }
    }

    @Test
    public void testWriteAndRead() throws Exception {
        Record record = new Record();
        PooledByteBuf byteBufLarge = (PooledByteBuf)ByteBuf.writeObjet(record);
        PooledByteBuf byteBufNormal = (PooledByteBuf)ByteBuf.writeObjet(record.student);
        logger.debug("record Buffer, 存在"+byteBufLarge.getType().name()+"的"+byteBufLarge.getIndex()+"位置");
        logger.debug("student Buffer, 存在"+byteBufNormal.getType().name()+"的"+byteBufNormal.getIndex()+"位置");
        Record record1 = byteBufLarge.readObject();
        Record record2 = byteBufLarge.readObjectAndRelease();
        Student student = byteBufNormal.readObjectAndRelease();
        logger.debug(record1.equals(record));
        logger.debug(record2.equals(record));
        logger.debug(student.equals(record.student));
    }

    @Test
    public void testUp() throws Exception {
        int i = 512;
        while (i >= 0) {
            Student student = new Student();
            ByteBuf.writeObjet(student);
            i--;
        }
    }

    @Test
    public void testMultiThread() throws Exception {
        List<ByteBuf> origin = addObject(100, new Student());
        CountDownLatch countDownLatch = new CountDownLatch(2);
        AtomicReference<List<ByteBuf>> t1 = new AtomicReference<>();
        AtomicReference<List<ByteBuf>> t2 = new AtomicReference<>();
        Thread thread1 = new Thread(()-> {
            try {
               t1.set(addObject(56, new Student()));
               countDownLatch.countDown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread thread2 = new Thread(()-> {
            try {
                t2.set(addObject(50, new Record()));
                countDownLatch.countDown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread1.start();
        thread2.start();
        countDownLatch.await();
        List<ByteBuf> unPooledT1 =  t1.get().stream().filter(byteBuf -> byteBuf instanceof UnPooledByteBuf).toList();
        List<ByteBuf> unPooledT2 =  t2.get().stream().filter(byteBuf -> byteBuf instanceof UnPooledByteBuf).toList();
        if(!unPooledT1.isEmpty()) {
            logger.debug("t1 unpooled: "+unPooledT1.size());
        }
        if(!unPooledT2.isEmpty()) {
            logger.debug("t2 unpooled: "+unPooledT2.size());
        }
        PooledByteBuf p = (PooledByteBuf) (origin.get(0));
        p.getAllocator().logInfo();
        boolean b = true;
        for(ByteBuf byteBuf : t2.get()) {
            Record o = byteBuf.readObjectAndRelease();
            if(!o.equals(new Record())) {
                b = false;
            }
        }
        if(b) {
            p.getAllocator().logInfo();
        }
    }

    @Test
    public void testQueueAdd() throws Exception {
        StmQueue deque = new StmQueue("hahah", 8, 160);
        CountDownLatch countDownLatch = new CountDownLatch(3);
        Thread t1 = new Thread(()-> {
            try {
                addObject(56, new Student(),deque);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            countDownLatch.countDown();
        });
        Thread t2 = new Thread(()-> {
            try {
                addObject(56, new Student(),deque);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            countDownLatch.countDown();
        });
        Thread t3 = new Thread(()->{
            for(int i = 0; i < 67; i++) {
                deque.take();
            }
            countDownLatch.countDown();
        });
        t1.start();
        t2.start();
        t3.start();
        countDownLatch.await();
        deque.log();
    }

    @Test
    public void testMultiReadWrite() throws IOException, InterruptedException, ClassNotFoundException {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        StmQueue deque = new StmQueue("hahah", 8, 160);
        BlockingDeque<Student> deque1 = new LinkedBlockingDeque<>();
        BlockingDeque<Record> deque2 = new LinkedBlockingDeque<>();
        List<ByteBuf> origin = addObject(100, new Student());
        Thread thread1 = new Thread(()-> {
            try {
                addObject(56, new Student(),deque);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread thread2 = new Thread(()-> {
            try {
                addObject(50, new Record(), deque);
                Record record = new Record();
                record.key = "end";
                addObject(2, record, deque);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Thread thread3 = new Thread(()->{
            try {
                while (true) {
                    Object object = deque.take().readObjectAndRelease();
                    if (object instanceof Record && ((Record) object).key.equals("end")) {
                        break;
                    }
                    if(object instanceof Student) {
                        deque1.offer((Student) object);
                    } else {
                        deque2.offer((Record) object);
                    }
                }
                countDownLatch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread thread4 = new Thread(()->{
            try {
                while (true) {
                    Object object = deque.take().readObjectAndRelease();
                    if (object instanceof Record && ((Record) object).key.equals("end")) {
                        break;
                    }
                    if(object instanceof Student) {
                        deque1.offer((Student) object);
                    } else {
                        deque2.offer((Record) object);
                    }
                }
                countDownLatch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        countDownLatch.await();
        PooledByteBuf p = (PooledByteBuf) (origin.get(0));
        p.getAllocator().logInfo();
        deque.log();
        List<Long> ids = new ArrayList<>();
        while (!deque1.isEmpty()) {
            ids.add(deque1.take().id);
        }
        List<Long> old = new ArrayList<>(ids);
        ByteBuf byteBuf = deque.tryTake();
        while (byteBuf != null) {
            Object o = byteBuf.readObjectAndRelease();
            if(o instanceof Student) {
                ids.add(((Student) o).id);
            } else {
                deque2.offer((Record) o);
            }
            byteBuf = deque.tryTake();
        }
        deque.log();
        old.sort(Long::compareTo);
        ids.sort(Long::compareTo);
        for(long id : old) {
            System.out.printf("%4d", id);
        }
        System.out.println();
        for(long id : ids) {
            System.out.printf("%4d", id);
        }
        System.out.println();
        logger.debug(deque2.size());
    }


    private List<ByteBuf> addObject(int n, Object o) throws IOException {
        List<ByteBuf> byteBufs = new ArrayList<>();
        for(int i = 0; i < n; i++) {
            byteBufs.add(ByteBuf.writeObjet(o));
        }
        return byteBufs;
    }

    private void addObject(int n, Object o, BlockingDeque<ByteBuf> deque) throws IOException {
        for(int i = 0; i < n; i++) {
            deque.offer(ByteBuf.writeObjet(o));
        }
    }

    private void addObject(int n, Object o, StmQueue deque) throws IOException {
        for(int i = 0; i < n; i++) {
            if(o instanceof Student) {
                ((Student) o).setId(i);
            }
            deque.offer(ByteBuf.writeObjet(o));
        }
    }

}

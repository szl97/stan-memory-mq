package tcase;

import com.stmq.consumer.StmConsumer;
import com.stmq.producer.StmProducer;
import com.stmq.server.manager.StmBroker;
import com.stmq.server.model.StmConsumerRecord;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Author: Stan sai
 * Date: 2024/2/24 02:10
 * description:
 */
public class TestProducerAndCustomer {
    static Logger logger = Logger.getLogger(TestProducerAndCustomer.class);
    static {
        try {
            StmBroker.start(128 * 1024, 256 * 1024);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testProducerAndConsumer() throws Exception {
        StmProducer producer = new StmProducer(StmBroker.getBROKER());
        LinkedBlockingDeque<Record> records = new LinkedBlockingDeque<>();
        LinkedBlockingDeque<Student> students1 = new LinkedBlockingDeque<>();
        LinkedBlockingDeque<Student> students2 = new LinkedBlockingDeque<>();
        CountDownLatch countDownLatch = new CountDownLatch(130);
        Thread t1 = new Thread(() -> {
            for(int i = 0; i < 50; i++) {
                try {
                    producer.send("student", new Student(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread t2 = new Thread(()->{
            for(int i = 0; i < 50; i++) {
                try {
                    producer.send("student", new Student(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread t3 = new Thread(()->{
            for(int i = 0; i < 30; i++) {
                try {
                    producer.send("record", new Record());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        StmConsumer consumer1 = new StmConsumer(StmBroker.getBROKER(), "student", "record") {
            @Override
            public <T> void consumer(StmConsumerRecord<T> record) {
                T data = record.getData();
                if(data instanceof Record) {
                    records.offer((Record) data);
                } else {
                    students1.offer((Student) data);
                }
                countDownLatch.countDown();
            }
        };

        StmConsumer consumer2 = new StmConsumer(StmBroker.getBROKER(), "student") {
            @Override
            public <T> void consumer(StmConsumerRecord<T> record) {
                students2.offer((Student) record.getData());
                countDownLatch.countDown();
            }
        };
        t1.start();
        t2.start();
        t3.start();

        countDownLatch.await();
        System.out.println("student1 num: " + students1.size());
        System.out.println("student2 num: " + students2.size());
        System.out.println("print student1 : ");
        while (!students1.isEmpty()) {
            System.out.printf("%4d", students1.takeFirst().id);
        }
        System.out.println();
        System.out.println("print student2 : ");
        while (!students2.isEmpty()) {
            System.out.printf("%4d", students2.takeFirst().id);
        }
        System.out.println();
        System.out.println("recods num: " + records.size());
    }

}

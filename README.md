# stan-memory-mq


## 内存池设计
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

## 队列设计

 * 数组+锁，实现阻塞队列
 * 读、写时只加读锁，不会阻塞
 * 扩容时加写锁，尽量通过配置足够长度来避免扩容
 * 阻塞的poll方法使用sleep和interrupt,使用了线程安全的ConcurrentLinkedQueue存放timed_waiting的消费者线程

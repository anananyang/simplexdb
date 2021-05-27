package buffer;

import buffer.bufferAssign.*;
import file.BlockId;
import file.FileManager;
import log.LogManager;

import java.util.Iterator;


public class BufferManager {

    // 根据不同的 buffer 分配策略来选择不同的数据结构, 比如 FIFI 或者 LRU 可以使用链表，clock 可以使用循环队列
    private BufferAssignStrategy assignStrategy;
    private int availableNum = 0;
    public static final long MAX_WAITING_TIME = 10000;  // 最长等待10秒

    public BufferManager(FileManager fileManager, LogManager logManager, int bufferSize) {
        this.availableNum = bufferSize;
//        assignStrategy = new NaiveStrategy(fileManager, logManager, bufferSize);
//        assignStrategy = new ClockStrategy(fileManager, logManager, bufferSize);
//        assignStrategy = new FIFOStrategy(fileManager, logManager, bufferSize);
        assignStrategy = new LRUStrategy(fileManager, logManager, bufferSize);   // 默认使用 LRU 的缓存分配策略
    }

    /**
     * 分配一个 buffer 给指定的 blk
     *
     * @param blk
     * @return
     */
    public synchronized Buffer pin(BlockId blk) {
        try {
            long startStamp = System.currentTimeMillis();
            Buffer buffer = this.tryToPin(blk);
            while (buffer == null && !this.waitingTooLong(startStamp)) {
                System.out.println(Thread.currentThread().getName() + " try to wait at " + System.currentTimeMillis());
                wait(MAX_WAITING_TIME);   // 等待 unpin 时唤醒
                System.out.println(Thread.currentThread().getName() + " retry to get buffer at " + System.currentTimeMillis());
                buffer = this.tryToPin(blk);
            }
            // 最终未获取到缓存
            if (buffer == null) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long startStamp) {
        return System.currentTimeMillis() - startStamp > MAX_WAITING_TIME;
    }


    /**
     * 尝试去 blk 获取一个缓存。这里面有两种情况
     * 1. 已经有一个缓存分配给指定block，此时将已经分配缓存直接返回即可
     * 2. 之前未分配过缓存给指定block, 则需要分配一个暂未分配的缓存给指定block
     *
     * @param blk
     * @return
     */
    private Buffer tryToPin(BlockId blk) {
        Buffer buffer = findExistingBuffer(blk);
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer();
            if (buffer == null) {
                return null;
            }
            // 将缓存分配给当前 block
            buffer.assignToBlk(blk);
        }
        // 如果之前缓存未分配，现在分配之后，可用缓存数量要 -1
        if (!buffer.isPinned()) {
            this.availableNum--;
        }
        buffer.pin();

        return buffer;
    }

    private Buffer chooseUnpinnedBuffer() {
        return assignStrategy.chooseUnpinnedBuffer();
    }

    /**
     * 当缓存数量很小时，这里使用循环遍历 buffer 是可以接受的
     * 当缓存数量很大时，可以修改成
     * @param blk
     * @return
     */
    private Buffer findExistingBuffer(BlockId blk) {
        Iterator<Buffer> it = assignStrategy.iterator();
        while(it.hasNext()) {
            Buffer buffer = it.next();
            if (blk.equals(buffer.getBlk())) {
                return buffer;
            }
        }

        return null;
    }

    /**
     * 缓存释放
     *
     * @param buffer
     * @return
     */
    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            assignStrategy.unpin(buffer);   // 根据不同分配策略进行管理，比如 LRU，在 unpin 时，需要将 buffer 放入buffer队列的尾部
            availableNum++;
            notifyAll();   // 已经有个可用的buffer, 唤醒其他线程去使用
        }
    }

    public synchronized int getAvailableNum() {
        return availableNum;
    }


    public void flushAll(int txtnum) {
        Iterator<Buffer> it = assignStrategy.iterator();
        while(it.hasNext()) {
            Buffer buffer = it.next();
            if (buffer.getModifyingTx() == txtnum) {
                buffer.flush();
            }
        }
    }
}

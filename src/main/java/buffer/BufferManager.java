package buffer;

import file.BlockId;
import file.FileManager;
import log.LogManager;


public class BufferManager {

    private Buffer[] buffers;   // 根据不同的 buffer 分配策略来选择不同的数据结构
    private int availableNum = 0;
    private static final long MAX_WAITING_TIME = 10000;  // 最长等待10秒

    public BufferManager(FileManager fileManager, LogManager logManager, int bufferSize) {
        this.availableNum = bufferSize;
        for (int i = 0; i < bufferSize; i++) {
            buffers[i] = new Buffer(fileManager, logManager);
        }
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
                wait(MAX_WAITING_TIME);   // 等待 unpin 时唤醒
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
            buffer = chooseUnpinnedBuffer(blk);
            if (buffer == null) {
                return null;
            }
        }
        // 如果之前缓存未分配，现在分配之后，可用缓存数量要 -1
        if (!buffer.isPinned()) {
            this.availableNum--;
        }
        buffer.pin();

        return buffer;
    }

    private Buffer chooseUnpinnedBuffer(BlockId blk) {
        for (Buffer buffer : buffers) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    /**
     * 当缓存数量很小时，这里使用循环遍历 buffer 是可以接受的
     * 当缓存数量很大时，可以修改成
     * @param blk
     * @return
     */
    private Buffer findExistingBuffer(BlockId blk) {
        for (Buffer buffer : buffers) {
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
            availableNum++;
            notifyAll();   // 已经有个可用的buffer, 唤醒其他线程去使用
        }
    }

    public synchronized int getAvailableNum() {
        return availableNum;
    }


    public void flushAll(int txtnum) {
        for (Buffer buffer : buffers) {
            if (buffer.getModifyingTx() == txtnum) {
                buffer.flush();
            }
        }
    }


}

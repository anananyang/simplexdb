package buffer;


import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;

public class Buffer {

    private FileManager fileManager;
    private LogManager logManager;
    private Page content;
    private BlockId blk;
    /**
     * 类似引用计数，当 pin = 0 时，表示该 buffer 是可用的。每次缓存分配之后需要累加 pin, buffer 还回时，pin 递减
     */
    private int pin = 0;
    // 事务ID，记录最后一次被修改过当前缓存的事务ID
    private int txnum = -1;
    // 最后一次被修改的日志序列号
    private int lsn = -1;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        content = new Page(fileManager.getBlockSize());
    }

    /**
     * 将当前缓存分配给一个新的 blk。在分配时，需要将之前 content 写回到磁盘。
     * 在分配时写回的原因是因为 unppinned 的 buffer 中的内容可能被新的 client 用到
     * 为了能够减少磁盘操作
     *
     * @param blockId
     */
    public void assignToBlk(BlockId blockId) {
        flush();
        this.blk = blockId;
        fileManager.read(blockId, content);
        pin = 0;
    }

    /**
     * 更新最后一次修改过当前缓存的事务ID，以及相应的日志序列号
     *
     * @param txnum
     * @param lsn
     */
    public void modifyTxnumAndLsn(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    public int getLsn() {
        return this.lsn;
    }

    public int getModifyingTx() {
        return this.txnum;
    }

    public BlockId getBlk() {
        return this.blk;
    }

    public Page getContent() {
        return this.content;
    }

    /**
     * 如果 content 内容被修改过，则将其写回磁盘
     */
    public void flush() {
        if (txnum < 0) {
            return;
        }
        // 先将日志写回磁盘，日志用于恢复操作，要先于内容写回磁盘
        logManager.flush(lsn);
        // 将缓存中的内容写回磁盘
        fileManager.write(blk, content);
        // 协会之后，将事务ID改回默认的状态
        this.txnum = -1;
    }

    public boolean isPinned() {
        return this.pin > 0;
    }

    public void pin() {
        this.pin++;
    }

    public void unpin() {
        this.pin--;
    }

}


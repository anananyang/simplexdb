package tx;

import buffer.Buffer;
import buffer.BufferManager;
import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import tx.concurrency.ConcurrencyManager;
import tx.recover.RecoverManager;
import tx.recover.logRecord.StartLogRecord;

/**
 * 事务主要是保证ACID性质
 * 1. 原子性
 * 2. 一致性
 * 3. 隔离性
 * 4. 持久性
 * 其中事务利用恢复管理器写log来保证事务都原子性和持久性；利用并发管理器来保证事务都隔离性和一致性
 * <p>
 * 可以将 Transaction 的方法分为三个部分
 * 1. 与 recoverManager 相关的操作
 * 2. 与 buffer 相关的操作
 * 3. 与 fileManager 相关的操作
 */
public class Transaction {

    private final static Integer END_OF_FILE = -1;
    private static int nextTxnum = -1;
    private int txnum;
    private FileManager fileManager;
    private LogManager logManager;
    private BufferManager bufferManager;
    // 管理当前事务获取到的所有的 buffer
    private BufferList myBufferList;
    // 管理当前事务获取到的所有的锁
    private ConcurrencyManager concurrencyManager;
    // 记录当前事务对于数据的操作
    private RecoverManager recoverManager;


    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {
        // 获取事务ID，需要在创建 recoverManager 之前执行，因为在 recoverManager 中需要用到
        this.txnum = getNextTxnum();

        this.fileManager = fileManager;
        this.logManager = logManager;
        this.bufferManager = bufferManager;

        // 创建支持当前事务的三个子组件: 恢复管理器、并发管理器、事务缓存管理器
        this.myBufferList = new BufferList(bufferManager);
        this.concurrencyManager = new ConcurrencyManager();
        this.recoverManager = new RecoverManager(this, logManager, bufferManager);
    }


    public int getTxnum() {
        return txnum;
    }

    /**
     * 事务提交
     */
    public void commit() {
        recoverManager.commit();
        System.out.println("transaction:" + txnum + " commited");

        // 这里先释放锁护着先释放缓存都可以，因为 recoverManager 已经将变更都写回磁盘了。
        // 但是先释放锁可以方便其他事务加锁，并且能够保证其他事务获取到锁时，buffer 的数据还在，不需要重新从磁盘中读取

        // 释放锁
        concurrencyManager.release();
        // 释放缓存
        myBufferList.unpinAll();

    }

    /**
     * 事务回滚
     */
    public void rollback() {
        recoverManager.rollback();
        System.out.println("transaction:" + txnum + " rolled back");
        // 释放锁
        concurrencyManager.release();
        // 释放缓存
        myBufferList.unpinAll();
    }

    /**
     * 恢复处理
     */
    public void recover() {
        // 当前使用的恢复策略是 undo-only, 这种策略要求所有 commit 的数据必须已经写回磁盘
        // 不过 simplexdb 的 recover 一般是在启动时执行。

        // 先将数据写回磁盘
        bufferManager.flushAll(txnum);
        // 恢复处理
        recoverManager.recover();
    }


    public void pin(BlockId blockId) {
        myBufferList.pin(blockId);

    }

    public void unpin(BlockId blockId) {
        myBufferList.unpin(blockId);
    }

    /**
     * 这里不需要 pin 一下 block, pin 的过程是很繁琐的。
     * 而 getInt 的调用会很频繁，这里要保证在调用 getInt 前
     * 已经 pin 过了，以减少 pin 的调用次数
     *
     * ps: 这里获取的锁是在事务提交或者回滚时统一释放
     * @param blk
     * @param offset
     * @return
     */
    public int getInt(BlockId blk, int offset) {
        concurrencyManager.sLock(blk);
        Buffer buffer = myBufferList.getBuffer(blk);
//        if(buffer == null) {
//
//        }
        return buffer.getContent().getInt(offset);
    }

    public void setInt(BlockId blk, int offset, int value, boolean okToLog) {
        concurrencyManager.xLock(blk);
        Buffer buffer = myBufferList.getBuffer(blk);
        int lsn = -1;
        if(okToLog) {
            lsn = recoverManager.setIntLog(buffer, offset, value);
        }
        Page page = buffer.getContent();
        page.setInt(offset, value);

        buffer.modifyTxnumAndLsn(txnum, lsn);
    }

    public String getString(BlockId blockId, int offset){
        concurrencyManager.sLock(blockId);
        Buffer buffer = myBufferList.getBuffer(blockId);
        return buffer.getContent().getString(offset);
    }

    /**
     *
     * @param blk
     * @param offset
     * @param value
     * @param okToLog: rollback 时 false
     */
    public void setString(BlockId blk, int offset, String value, boolean okToLog) {
        concurrencyManager.xLock(blk);
        Buffer buffer = myBufferList.getBuffer(blk);
        int lsn = -1;
        if(okToLog) {
            lsn = recoverManager.setStringLog(buffer, offset, value);
        }
        Page page = buffer.getContent();
        page.setString(offset, value);

        buffer.modifyTxnumAndLsn(txnum, lsn);
    }

    /**
     * 获取缓存管理器中可用的缓存数量
     *
     * @return
     */
    public int availableBuffs() {
        return bufferManager.getAvailableNum();
    }

    /**
     * 根据 blockSize 计算文件的块长度
     *
     * @return
     */
    public int size(String fileName) {
        BlockId blk = new BlockId(fileName, END_OF_FILE);
        // 读取文件的块长读，所以加共享锁
        concurrencyManager.sLock(blk);
        return fileManager.getNewBlockNum(fileName);
    }

    public BlockId append(String fileName) {
        BlockId blk = new BlockId(fileName, END_OF_FILE);
        // 这里是写文件，必须加独占锁
        concurrencyManager.xLock(blk);
        return fileManager.append(fileName);
    }

    public int blockSize() {
        return fileManager.getBlockSize();
    }

    // 生成事务ID的静态方法, 需要同步获取
    private static synchronized int getNextTxnum() {
        nextTxnum = nextTxnum + 1;
        return nextTxnum;
    }


}

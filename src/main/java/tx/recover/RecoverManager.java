package tx.recover;

import buffer.Buffer;
import buffer.BufferManager;
import file.BlockId;
import log.LogManager;
import tx.Transaction;
import tx.recover.logRecord.*;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * 1. 恢复管理器的作用
 * 主要用于系统启动时，将系统恢复到可信赖的状态，那么什么是可信赖的状态呢？
 * * 在日志记录中，所有 uncommitted 的事务都被回滚
 * * 所有已提交的事务都已经被写回到磁盘文件中
 * <p>
 * 2. 恢复策略
 * 一般来说有三种恢复策略
 * undo-redo
 * * undo-only
 * * redo-only
 * simplexdb 选择使用 undo-only，使用 undo-only 的话需要考虑一下几个条件
 * 1. 需要从后向前读取logfile的中的logRecord
 * 2. 需要保证已经commit的数据已经被写回磁盘
 * 3. 每条 update record 都要优先于 blockBuffer 写回磁盘
 * <p>
 * 3. checkPoint
 * 这里以 undo-only 为例，如果没有 checkPoint，那么在执行 recover 时，需要读取整个文件。而如果存在 checkPoint 的话，只需要读取到
 * checkPoint record 为主
 * 根据 checkPoint 写入实际，可以分成 checkRecord，一种是静态checkRecord，其在系统每次执行完 recover 操作之后写入到 logFile 中。
 * 静态checkPoint的优势就是比较容易理解，并且实现起来也非常简单。因为系统只在启动时才去做 recover 操作，这可能导致的结果每次启动时，需
 * 要处理的 logRecore 非常长，在 recover 期间，数据库系统时不能接受外部请求的，如果 recover 的时间非常长，是不可人忍受的，所以开发出一种
 * 非静态checkPoint的方法
 * simplexdb 基于掌握数据库内部结构的目的，所以这里使用静态checkPoint
 */
public class RecoverManager {

    private LogManager logManager;
    private BufferManager bufferManager;
    private Transaction tx;
    private int txnum;

    /**
     * undo-only 要求在将 commit 的log record 写回磁盘前，要先将修改的记录写回到磁盘
     */
    public void commit() {
        // 先将修改的记录刷回到磁盘
        bufferManager.flushAll(txnum);
        // 写入 commit log record
        int lsn = CommitLogRecord.writeToLog(logManager, txnum);
        // 将 commit log record 写回磁盘
        logManager.flush(lsn);
    }

    public void rollback() {
        this.doRollback();
        bufferManager.flushAll(txnum);
        int lsn = RollbackLogRecord.writeToLog(logManager, txnum);
        logManager.flush(lsn);
    }

    /**
     * 对指定 txnum 做 rollback
     */
    private void doRollback() {
        Iterator<byte[]> it = logManager.iterator();
        while (it.hasNext()) {
            byte[] bytes = it.next();
            LogRecord logRecord = LogRecordFactory.createLogRecord(bytes);
            // 只有事务ID一样，才进行处理
            if (logRecord.txnum() == txnum) {
                // 当处理到 StART log record 时 rollback 处理结束
                if (logRecord.type() == LogRecord.START) {
                    return;
                }
                // undoes
                logRecord.undo(tx);
            }
        }
    }


    public void recover() {
        this.doRecover();
        bufferManager.flushAll(txnum);
        int lsn = CheckPointLogRecord.writeToLog(logManager);
        logManager.flush(lsn);

    }

    private void doRecover() {
        Set<Integer> finishedTx = new TreeSet<>();
        Iterator<byte[]> it = logManager.iterator();
        while (it.hasNext()) {
            byte[] bytes = it.next();
            LogRecord logRecord = LogRecordFactory.createLogRecord(bytes);
            // 处理到最近一条 checkPoint 记录
            if (logRecord.type() == LogRecord.CHECK_POINT) {
                return;
            } else if (logRecord.type() == LogRecord.ROLLBACK || logRecord.type() == LogRecord.COMMIT) {
                finishedTx.add(logRecord.txnum());
            } else if (!finishedTx.contains(logRecord.txnum())) {
                // 原则上，在处理到 Start log record 时，如果 txnum 不在 finishedTx 中的话，需要添加一条 rollback 记录
                if (logRecord.type() == LogRecord.START) {
                    RollbackLogRecord.writeToLog(logManager, logRecord.txnum());
                } else {
                    logRecord.undo(tx);
                }
            }
        }
    }


    /**
     * 写一条 SET_INT 日志
     * NOTE：undo-only 只记录 oldVal
     *
     * @param buffer
     * @param offset
     * @param newVal
     */
    public int setIntLog(Buffer buffer, int offset, int newVal) {
        int oldVal = buffer.getContent().getInt(offset);
        BlockId blk = buffer.getBlk();
        return SetIntLogRecord.writeToLog(logManager, txnum, blk, offset, oldVal);
    }

    /**
     * 写一条 SET_STRING 日志
     *
     * @param buffer
     * @param offset
     * @param newVal
     */
    public int setStringLog(Buffer buffer, int offset, String newVal) {
        String oldVal = buffer.getContent().getString(offset);
        BlockId blockId = buffer.getBlk();
        return SetStringLogRecord.writeToLog(logManager, txnum, blockId, offset, oldVal);
    }

}

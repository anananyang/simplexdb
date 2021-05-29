package log;

import file.BlockId;
import file.FileManager;
import file.Page;

import java.util.Iterator;

/**
 * 日志的作用是用于记录用户的操作，当用户需要撤销某个操作是，可以通过日志来进行恢复.
 * 正常情况下，日志文件的最后一个 block 都缓存在 logPage 中，只有以下两种情况，才需要将 logPage 中的内容持久化到文件中
 * 1. logPage 空间不足
 * 2. 强制将 logPage 中的内容写回到 logFile
 *
 * Note: 日志管理器并不需要知道日志的格式
 */
public class LogManager {

    private FileManager fileManager;
    private String logFileName;

    // logPage 永远缓存日志文件的最后一个 block 的内容
    private Page logPage;
    private BlockId currentBlk;

    // 日志序列号
    private int lastestLSN = 0;
    // 最后一次写回到文件到日志序列号
    private int lastestSavedLSN = 0;


    public LogManager(FileManager fileManager, String logFileName) {
        this.fileManager = fileManager;
        this.logFileName = logFileName;
        // 分配一个 Page
        logPage = new Page(new byte[fileManager.getBlockSize()]);
        // 文件内容的长度
        int logFileNewBlockNum = fileManager.getNewBlockNum(logFileName);
        // 如果文件没有内容
        if (logFileNewBlockNum == 0) {
            // 构造一个新的块
            currentBlk = appendNewBlock();
        } else {
            int lastBlockNum = logFileNewBlockNum - 1;   // 计算文件的最后一个块
            currentBlk = new BlockId(logFileName, lastBlockNum);
            fileManager.read(currentBlk, logPage);  // 将块内容读取到 page 中
        }
    }

    /**
     * 添加一条记录，每个 logPage 的前四个字节存储当前 logPage 的 boundary
     * 写入时，从 logPage 的最后开始写入
     * ps: 这里并没有处理日志记录长度超过 blockSize 的情况，实际上可能会出现这种情况，如果 logRecord 的类型是 SET_STRING
     * @param logRec
     * @return
     */
    public synchronized int append(byte[] logRec) {
        // 先判断 logPage 空间是否足够
        int boundary = logPage.getInt(0);
        int logRecLen = logRec.length;
        int bytesNeeded = logRecLen + Integer.BYTES;   // 写字节总共需要的长度
        // 如果存储空间不足
        if (boundary - bytesNeeded < Integer.BYTES) {
            this.flush();   // 将 logPage 写回到 logFile
            currentBlk = this.appendNewBlock();   // 申请一个新的块, 并重设 logPage 的 boundary.
            boundary = logPage.getInt(0);
        }
        // 计算写入的位置
        int writeOffset = boundary - bytesNeeded;
        logPage.setBytes(writeOffset, logRec);
        // 重设 boundary
        logPage.setInt(0, writeOffset);
        // 写成功之后的处理
        lastestLSN = lastestLSN + 1;
        return lastestLSN;
    }

    /**
     * ps: 这里的的 迭代器是从 logfile 的最后一条日志向前读，这样读的原因是为来配置 recoverManager 的 recover 策略
     * recover 共有三种策略
     * 1. undo-redo: 先执行一遍 undo，再执行一遍 redo
     * 2. undo-only: 将 logfile 中的 uncommit 和 umrollback 的 updateRecord 中的事务用 oldValue 写回指定文件指定block指定
     *               offse, 这需要从最后一条 logRecord 开始向前读
     * 3. redo-only: 将 logfile 中已经 commit 的事务重新执行一遍，这需要从logfile的第一条记录向后读。
     *
     * simplexdb 选用的恢复方式是 undo-only 的方式，所以从后向前读读方式
     *
     *
     * @return
     */
    public Iterator<byte[]> iterator() {
        // 在迭代前将 logPage 写回到日志文件
        this.flush();
        return new LogIterator(fileManager, currentBlk);
    }


    /**
     * 保证将至少到 lsn 中到数据写回到 logFile
     *
     * @param lsn
     */
    public void flush(int lsn) {
        if (lsn > lastestSavedLSN) {
            flush();
        }
    }

    /**
     * 将 page 写回到文件中
     */
    private void flush() {
        fileManager.write(currentBlk, logPage);
        this.lastestSavedLSN = this.lastestLSN;
    }


    private BlockId appendNewBlock() {
        // 生成一个新块
        BlockId blk = fileManager.append(logFileName);
        // 清空一下 logPage。logPage 的前四个字段表示当前块的边界
        logPage.setInt(0, fileManager.getBlockSize());
        // 将page写回到文件中（现在 page 中只记录了page的边界）
        fileManager.write(blk, logPage);

        return blk;
    }


}

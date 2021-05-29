package tx.recover.logRecord;

import file.Page;
import log.LogManager;
import tx.Transaction;

/**
 * checkPoint logRecord 只有 type(Integer)
 */
public class CheckPointLogRecord implements LogRecord {

    @Override
    public int type() {
        return CHECK_POINT;
    }

    /**
     * checkPoint 没有 txnum
     *
     * @return
     */
    @Override
    public int txnum() {
        return -1;
    }

    @Override
    public void undo(Transaction tx) {
        // do nothing
    }

    public String toString() {
        return "<CHECKPOINT>";
    }

    /**
     * 写入一条 checkPoint 到日志文件中
     *
     * @param logManager
     */
    public static int writeToLog(LogManager logManager) {
        byte[] bytes = new byte[Integer.BYTES];
        new Page(bytes).setInt(0, CHECK_POINT);
        // 写到日志中
        return logManager.append(bytes);
    }




}

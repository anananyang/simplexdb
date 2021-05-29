package tx.recover.logRecord;

import file.Page;
import log.LogManager;
import tx.Transaction;

/**
 * start log record 格式如下<type(Integer),txnum(Integer)>
 */
public class StartLogRecord implements LogRecord {

    private int txnum;

    public StartLogRecord(Page page) {
        int txnumPos = 0 + Integer.BYTES;
        this.txnum = page.getInt(txnumPos);
    }

    @Override
    public int type() {
        return START;
    }

    @Override
    public int txnum() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        // do nothing
    }

    public String toString() {
        return "<START " + txnum + ">";
    }

    /**
     * 写入一条 start log record
     *
     * @param logManager
     * @param txnum
     */
    public static int writeToLog(LogManager logManager, int txnum) {
        int typePos = 0;  // log record 类型写入的位置
        int txnumPos = typePos + Integer.BYTES; // txnum 写入位置
        int recordLen = txnumPos + Integer.BYTES;
        byte[] rec = new byte[recordLen];
        // 将日志类型和事务ID写入到 byte[] 中
        Page page = new Page(rec);
        page.setInt(typePos, START);
        page.setInt(txnumPos, txnum);

        // 将日志记录写入到日志 Page 的content中
        return logManager.append(rec);
    }
}

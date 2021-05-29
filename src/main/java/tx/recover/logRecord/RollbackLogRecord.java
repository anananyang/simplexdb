package tx.recover.logRecord;

import file.Page;
import log.LogManager;
import tx.Transaction;

/**
 * Rollback log record 格式如下 <type(Integer), txnum(Integer)>
 */
public class RollbackLogRecord implements LogRecord {

    private int txnum;

    public RollbackLogRecord(Page page) {
        int txnumPos = 0 + Integer.BYTES;
        this.txnum = page.getInt(txnumPos);
    }

    @Override
    public int type() {
        return ROLLBACK;
    }

    @Override
    public int txnum() {
        return this.txnum;
    }

    public String toString() {
        return "<ROLLBACK " + txnum + ">";
    }

    @Override
    public void undo(Transaction tx) {
        // do nothing
    }

    /**
     * 写入一条 Rollback log record
     *
     * @param logManager
     * @param txnum
     * @return
     */
    public static int writeToLog(LogManager logManager, int txnum) {
        int typePos = 0;
        int txnumPos = typePos + Integer.BYTES;
        int recordLen = txnumPos + Integer.BYTES;

        byte[] rec = new byte[recordLen];
        Page page = new Page(rec);
        page.setInt(typePos, ROLLBACK);
        page.setInt(txnumPos, txnum);

        return logManager.append(rec);
    }

}

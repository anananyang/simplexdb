package tx.recover.logRecord;

import file.Page;
import log.LogManager;
import tx.Transaction;

/**
 * commit log record 格式如下 <type(Integer), txnum(Integer)>
 */
public class CommitLogRecord implements LogRecord {

    private int txnum;

    public CommitLogRecord(Page page) {
        int txnumPos = 0 + Integer.BYTES;
        this.txnum = page.getInt(txnumPos);
    }

    @Override
    public int type() {
        return COMMIT;
    }

    @Override
    public int txnum() {
        return this.txnum;
    }

    public String toString() {
        return "<COMMIT " + txnum + ">";
    }

    @Override
    public void undo(Transaction tx) {
        // do nothing
    }

    /**
     * 写入一条 commit log record
     *
     * @param logManager
     * @param txum
     * @return
     */
    public static int writeToLog(LogManager logManager, int txum) {
        int typePos = 0;
        int txnumPos = typePos + Integer.BYTES;
        int recordLen = txnumPos + Integer.BYTES;

        byte[] rec = new byte[recordLen];
        Page page = new Page(rec);
        page.setInt(typePos, COMMIT);
        page.setInt(txnumPos, txum);

        return logManager.append(rec);
    }
}

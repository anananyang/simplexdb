package tx.recover.logRecord;

import file.BlockId;
import file.Page;
import log.LogManager;
import tx.Transaction;

public class SetStringLogRecord implements LogRecord {

    private int txnum;
    private BlockId blk;
    private int offset;
    private String val;

    // <SET_STRING txnum fileName blockSize offset val>
    private static final String SET_STRING_LOG_RECORD_FORMAT = "<SET_STRING %d %s %d %d %s>";

    public SetStringLogRecord(Page page) {
        int typePos = 0;
        int txnumPos = typePos + Integer.BYTES;
        int fileNamePos = txnumPos + Integer.BYTES;

        String fileName = page.getString(fileNamePos);

        int blockNumPos = fileNamePos + Integer.BYTES;
        int offsetPos = blockNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        int blockSize = page.getInt(blockNumPos);
        this.blk = new BlockId(fileName, blockSize);
        this.txnum = page.getInt(txnumPos);
        this.offset = page.getInt(offsetPos);
        this.val = page.getString(valPos);

    }


    @Override
    public int type() {
        return SET_STRING;
    }

    @Override
    public int txnum() {
        return txnum;
    }


    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false);
        tx.unpin(blk);
    }

    public String toString() {
        return String.format(SET_STRING_LOG_RECORD_FORMAT,
                txnum,
                blk.getFileName(),
                blk.getBlockNum(),
                offset,
                val);
    }

    /**
     * 写日志
     *
     * @param logManager
     * @param txnum
     * @param blk
     * @param offset
     * @param val
     * @return
     */
    public static int writeToLog(LogManager logManager,
                                 int txnum,
                                 BlockId blk,
                                 int offset,
                                 String val) {
        int typePos = 0;
        int txnumPos = typePos + Integer.BYTES;
        int fileNamePos = txnumPos + Integer.BYTES;
        int blockNumPos = fileNamePos + Page.maxLength(blk.getFileName().length());
        int offsetPos = blockNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;
        int recordLen = valPos + Page.maxLength(val.length());

        byte[] rec = new byte[recordLen];
        Page page = new Page(rec);
        page.setInt(typePos, SET_STRING);
        page.setInt(txnumPos, txnum);
        page.setString(fileNamePos, blk.getFileName());
        page.setInt(blockNumPos, blk.getBlockNum());
        page.setInt(offsetPos, offset);
        page.setString(valPos, val);

        return logManager.append(rec);
    }


}

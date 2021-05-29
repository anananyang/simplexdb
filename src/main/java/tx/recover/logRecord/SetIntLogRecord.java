package tx.recover.logRecord;

import buffer.Buffer;
import file.BlockId;
import file.Page;
import log.LogManager;
import tx.Transaction;

import javax.jnlp.IntegrationService;

/**
 * SetInt 日志记录格式：<type(Integer), txnum(Integer), fileName(String), blockSize(Integer), offset(Integer), val(Integer)>
 */
public class SetIntLogRecord implements LogRecord {

    private int txnum;
    private int offset;  // blockId 对应的 Page 的位置
    private int val;     // undo-only, 这里的 val 是 oldVal
    private BlockId blk;

    // <SET_INT txnum fileName blockNum offset val>
    private final static String SET_INT_LOG_RECORD_FORAMT = "<SET_INT %d %s %d %d %d>";

    /**
     * 从 Page 中取出相应的数据。初始化到当前变量中
     *
     * @param page
     */
    public SetIntLogRecord(Page page) {
        int typePos = 0;
        int txnumPos = typePos + Integer.BYTES;
        int fileNamePos = txnumPos + Integer.BYTES;
        // 因为无法确定 fileName 的长度，所以要先取出 fileName 之后，才能确定 blockSize 的位置
        String fileName = page.getString(fileNamePos);
        // 计算剩余变量的长度
        int blockSizePos = fileNamePos + Page.maxLength(fileName.length());
        int offsetPos = blockSizePos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        int blockNum = page.getInt(blockSizePos);
        this.blk = new BlockId(fileName, blockNum);
        this.txnum = page.getInt(txnumPos);
        this.offset = page.getInt(offsetPos);
        this.val = page.getInt(valPos);
    }

    @Override
    public int type() {
        return SET_INT;
    }

    @Override
    public int txnum() {
        return this.txnum;
    }

    /**
     * 将 oldVal 写回到
     *
     * @param tx
     */
    @Override
    public void undo(Transaction tx) {
        // tx 内部会维护一个pin到的bufferList，在 commit 时,unpin 所有的 buffer
        tx.pin(blk);
        // 重写, 并且不生成日志
        tx.setInt(blk, offset, val, false);
        // 释放
        tx.unpin(blk);

    }

    public String toString() {
        return String.format(SET_INT_LOG_RECORD_FORAMT, txnum, blk.getFileName(), blk.getBlockNum(), offset, val);
    }

    /**
     * 写一条 SetInt 日志记录
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
                                 int val) {

        int typePos = 0;
        int txnumPos = 0 + Integer.BYTES;
        int fileNamePos = txnumPos + Integer.BYTES;
        int blockNumPos = fileNamePos + Page.maxLength(blk.getFileName().length());
        int offsetPos = blockNumPos + Integer.BYTES;
        int valPos = offsetPos + Integer.BYTES;

        int recordLen = valPos + Integer.BYTES;
        byte[] rec = new byte[recordLen];
        Page page = new Page(rec);

        page.setInt(typePos, SET_INT);
        page.setInt(txnumPos, txnum);
        page.setString(fileNamePos, blk.getFileName());
        page.setInt(blockNumPos, blk.getBlockNum());
        page.setInt(offsetPos, offset);
        page.setInt(valPos, val);

        return logManager.append(rec);
    }
}

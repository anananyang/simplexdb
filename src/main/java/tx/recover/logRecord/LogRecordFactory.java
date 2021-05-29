package tx.recover.logRecord;

import file.Page;

public abstract class LogRecordFactory {

    /**
     * 根据日志类型创建相应类型的日志记录
     * @param bytes
     * @return
     */
    public static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);
        int logRecordType = page.getInt(0);
        switch(logRecordType) {
            case LogRecord.START:
                return new StartLogRecord(page);
            case LogRecord.ROLLBACK:
                return new RollbackLogRecord(page);
            case LogRecord.COMMIT:
                return new CommitLogRecord(page);
            case LogRecord.SET_INT:
                return new SetIntLogRecord(page);
            case LogRecord.SET_STRING:
                return new SetStringLogRecord(page);
            case LogRecord.CHECK_POINT:
                return new CheckPointLogRecord();
            default:
                return null;
        }
    }
}

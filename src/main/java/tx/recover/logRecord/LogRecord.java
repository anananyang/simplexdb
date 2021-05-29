package tx.recover.logRecord;

import tx.Transaction;

public interface LogRecord {
    // type of log record
    int START = 0, ROLLBACK = 1, COMMIT = 2, SET_INT = 3, SET_STRING = 4, CHECK_POINT = 5;

    /**
     * 返回日志记录的类型
     *
     * @return
     */
    int type();


    /**
     * 当前日志记录所属的事务ID
     *
     * @return
     */
    int txnum();


    /**
     * simplexdb 使用 undo-only 的恢复策略
     *
     * @param tx
     */
    void undo(Transaction tx);




}

package tx.recover.logRecord;


import org.junit.Assert;
import org.junit.Test;
import tx.BaseTxTest;

import java.util.Iterator;

public class CheckPointLogRecordTest extends BaseTxTest {


    @Test
    public void testAWriteCheckPointLogRecord() {
        int lsn = CheckPointLogRecord.writeToLog(logManager); // 写一条日志，此时日志还在 logManager 的 logPage 中，没写回磁盘
        // 将日志写回磁盘
        logManager.flush(lsn);
        // 读取日志
        Iterator<byte[]> it = logManager.iterator();
        // 期待此时有一条日志
        Assert.assertTrue(it.hasNext());
        // 读取一条日志
        byte[] logRecordRec = it.next();
        LogRecord logRecord = LogRecordFactory.createLogRecord(logRecordRec);
        // 日志存在
        Assert.assertNotNull(logRecord);
        // 日志类型是 Start
        Assert.assertEquals(LogRecord.CHECK_POINT, logRecord.type());

        Assert.assertEquals("<CHECKPOINT>", logRecord.toString());

        Assert.assertFalse(it.hasNext());
    }
}

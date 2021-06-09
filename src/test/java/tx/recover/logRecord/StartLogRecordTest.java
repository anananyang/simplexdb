package tx.recover.logRecord;


import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

public class StartLogRecordTest extends BaseLogRecordTest{

    @Test
    public void testAWriteStartLogRecord() {
        // 随机生成事务ID
        int txnum = ThreadLocalRandom.current().nextInt(1000);
        System.out.println("生成一个START事务ID：" + txnum);
        int lsn = StartLogRecord.writeToLog(logManager, txnum); // 写一条日志，此时日志还在 logManager 的 logPage 中，没写回磁盘
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
        Assert.assertEquals(LogRecord.START, logRecord.type());
        // 日志的事务ID
        Assert.assertEquals(txnum, logRecord.txnum());

        Assert.assertEquals("<START " + txnum + ">", logRecord.toString());

        Assert.assertFalse(it.hasNext());
    }

}

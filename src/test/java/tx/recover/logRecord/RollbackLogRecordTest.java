package tx.recover.logRecord;


import file.BlockId;
import file.Page;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import server.SimplexDB;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RollbackLogRecordTest extends BaseLogRecordTest {

//    private static final String TEST_DATA_FILE = "rollback.dat";

    /**
     * 按照方法名升序，第一个执行
     */
    @Test
    public void testAWriteRollbackLogRecord() {
        System.out.println("start testAWriteRollbackLogRecord");
        // 随机生成事务ID
        int txnum = ThreadLocalRandom.current().nextInt(1000);
        System.out.println("生成一个ROLLBACK事务ID：" + txnum);
        int lsn = RollbackLogRecord.writeToLog(logManager, txnum); // 写一条日志，此时日志还在 logManager 的 logPage 中，没写回磁盘
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
        Assert.assertEquals(LogRecord.ROLLBACK, logRecord.type());
        // 日志的事务ID
        Assert.assertEquals(txnum, logRecord.txnum());

        Assert.assertEquals("<ROLLBACK " + txnum + ">", logRecord.toString());

        Assert.assertFalse(it.hasNext());
    }

//    /**
//     * 对 UNDO 方法进行测试
//     */
//    public void testBUndo() {
//        // 先生成测试数据
//        BlockId block = fileManager.append(TEST_DATA_FILE);
//        byte[] bytes = new byte[SimplexDB.DEFAULT_BLK_SIZE];
//        Page page = new Page(bytes);
//        fileManager.read(block, page);
//
//        // 先写入一个数据
//        int randomOffset = ThreadLocalRandom.current().nextInt(100);
//        int randomVal = ThreadLocalRandom.current().nextInt(1000);
//        page.setInt(randomOffset, randomVal);   // 在任意位置吸入
//
//
//    }


}

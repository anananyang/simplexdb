package tx.recover.logRecord;

import file.BlockId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import tx.BaseTxTest;

import java.io.File;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class SetStringLogRecordTest extends BaseTxTest {

    private static final String SET_INT_TEST_FILE_A = "writeSetIntTest.dat";

    /**
     * 删除文件
     */
    @BeforeClass
    public static void init() {
        File testFileA = new File(dir, SET_INT_TEST_FILE_A);
        if(testFileA.exists()) {
            testFileA.delete();
        }

//        File testFileB = new File(dir, SET_INT_TEST_FILE_B);
//        if(testFileB.exists()) {
//            testFileB.delete();
//        }
    }

    @AfterClass
    public static void clear() {
        File testFileA = new File(dir, SET_INT_TEST_FILE_A);
        if(testFileA.exists()) {
            testFileA.delete();
        }

//        File testFileB = new File(dir, SET_INT_TEST_FILE_B);
//        if(testFileB.exists()) {
//            testFileB.delete();
//        }
    }

    /**
     * 按照方法名升序，第一个执行
     */
    @Test
    public void testAWriteSetStringLogRecord() {
        // 随机生成事务ID
        int txnum = ThreadLocalRandom.current().nextInt(1000);
        System.out.println("生成一个SET_STRING事务ID：" + txnum);

        int blockNum = ThreadLocalRandom.current().nextInt(3);
        BlockId blockId = new BlockId(SET_INT_TEST_FILE_A, blockNum);
        int offset = ThreadLocalRandom.current().nextInt(100);
        String val = UUID.randomUUID().toString();
        // 写一条日志，此时日志还在 logManager 的 logPage 中，没写回磁盘
        int lsn = SetStringLogRecord.writeToLog(logManager, txnum, blockId, offset, val);
        // 将日志写回磁盘
        logManager.flush(lsn);
        // 读取日志
        Iterator<byte[]> it = logManager.iterator();
        // 期待此时有一条日志
        Assert.assertTrue(it.hasNext());
        // 读取一条日志
        byte[] logRecordRec = it.next();
        SetStringLogRecord logRecord = (SetStringLogRecord) LogRecordFactory.createLogRecord(logRecordRec);
        // 日志存在
        Assert.assertNotNull(logRecord);
        // 日志类型是 Start
        Assert.assertEquals(LogRecord.SET_STRING, logRecord.type());
        // 日志的事务ID
        Assert.assertEquals(txnum, logRecord.txnum());
        // fileName
        Assert.assertEquals(SET_INT_TEST_FILE_A, logRecord.getBlk().getFileName());
        // blockNum
        Assert.assertTrue(blockNum == logRecord.getBlk().getBlockNum());
        // offset
        Assert.assertTrue(offset == logRecord.getOffset());
        // val
        Assert.assertEquals(val, logRecord.getVal());

        Assert.assertEquals("<SET_STRING "
                        + txnum + " "
                        + logRecord.getBlk().getFileName() + " "
                        + logRecord.getBlk().getBlockNum() + " "
                        + logRecord.getOffset() + " "
                        + logRecord.getVal() + ">",
                logRecord.toString());

        Assert.assertFalse(it.hasNext());
    }
}

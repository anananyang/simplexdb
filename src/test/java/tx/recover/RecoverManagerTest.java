package tx.recover;

import buffer.Buffer;
import file.BlockId;
import file.Page;
import org.junit.*;
import org.junit.runners.MethodSorters;
import tx.BaseTxTest;
import tx.Transaction;

/**
 * 对恢复管理器进行测试
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)   // 按照名字升序执行
public class RecoverManagerTest extends BaseTxTest {

    private static final int[] arr = new int[]{ 5, 203, 30, 4, 20, 89};
    private static final String strA = "beforeA";
    private static final String strB = "beforeB";

    @Before
    public void before() {
        System.out.println("before");
        BlockId blkA = new BlockId("recoverTest.txt", 0);
        BlockId blkB = new BlockId("recoverTest.txt", 1);
        Transaction txA = new Transaction(fileManager, logManager, bufferManager);
        Transaction txB = new Transaction(fileManager, logManager, bufferManager);
        txA.pin(blkA);
        txB.pin(blkB);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            txA.setInt(blkA, pos, arr[i], true);
            txB.setInt(blkB, pos, arr[i], true);
            pos = pos + Integer.BYTES;
        }
        txA.setString(blkA, 30, strA, true);
        txB.setString(blkB, 30, strB, true);

        txA.commit();
        txB.commit();
    }

    @Test
    public void testA1Commit() {
        System.out.println("testA1Commit");
        // 写数据
        BlockId blkA = new BlockId("recoverTest.txt", 0);
        BlockId blkB = new BlockId("recoverTest.txt", 1);
        Transaction txA = new Transaction(fileManager, logManager, bufferManager);
        Transaction txB = new Transaction(fileManager, logManager, bufferManager);
        txA.pin(blkA);
        txB.pin(blkB);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            txA.setInt(blkA, pos, pos + i, true);
            txB.setInt(blkB, pos, pos + i, true);
            pos = pos + Integer.BYTES;
        }
        txA.setString(blkA, 30, "commitA", true);
        txB.setString(blkB, 30, "commitB", true);

        txA.commit();
        txB.commit();

        // 读取txA的写入的数据
        pos = 0;
        byte[] byteA = new byte[fileManager.getBlockSize()];
        byte[] byteB = new byte[fileManager.getBlockSize()];
        Page pageA = new Page(byteA);
        Page pageB = new Page(byteB);
        fileManager.read(blkA, pageA);
        fileManager.read(blkB, pageB);
        for (int i = 0; i < 6; i++) {
            Integer valA = pageA.getInt(pos);
            Integer valB = pageB.getInt(pos);
            Assert.assertTrue(valA.equals(pos + i));
            Assert.assertTrue(valB.equals(pos + i));
            pos = pos + Integer.BYTES;
        }
        Assert.assertEquals("commitA", pageA.getString(30));
        Assert.assertEquals("commitB", pageB.getString(30));
    }

    @Test
    public void testB2Rollback() {
        System.out.println("testB2Rollback");
        // 写数据
        BlockId blkA = new BlockId("recoverTest.txt", 0);
        BlockId blkB = new BlockId("recoverTest.txt", 1);
        Transaction txA = new Transaction(fileManager, logManager, bufferManager);
        Transaction txB = new Transaction(fileManager, logManager, bufferManager);
        txA.pin(blkA);
        txB.pin(blkB);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            txA.setInt(blkA, pos, pos + i + 1, true);
            txB.setInt(blkB, pos, pos + i + 1, true);
            pos = pos + Integer.BYTES;
        }
        txA.setString(blkA, 30, "commitA", true);
        txB.setString(blkB, 30, "commitB", true);

        txA.rollback();
        txB.rollback();

        // 读取txA的写入的数据
        pos = 0;
        byte[] byteA = new byte[fileManager.getBlockSize()];
        byte[] byteB = new byte[fileManager.getBlockSize()];
        Page pageA = new Page(byteA);
        Page pageB = new Page(byteB);
        fileManager.read(blkA, pageA);
        fileManager.read(blkB, pageB);
        for (int i = 0; i < 6; i++) {
            Integer valA = pageA.getInt(pos);
            Integer valB = pageB.getInt(pos);
            Assert.assertTrue(valA.equals(arr[i]));
            Assert.assertTrue(valB.equals(arr[i]));
            pos = pos + Integer.BYTES;
        }
        Assert.assertEquals(strA, pageA.getString(30));
        Assert.assertEquals(strB, pageB.getString(30));
    }

    @Test
    public void testC3Recover() {
        System.out.println("testC3Recover");
        // 写数据
        BlockId blkA = new BlockId("recoverTest.txt", 0);
        BlockId blkB = new BlockId("recoverTest.txt", 1);
        Transaction txA = new Transaction(fileManager, logManager, bufferManager);
        Transaction txB = new Transaction(fileManager, logManager, bufferManager);
        txA.pin(blkA);
        txB.pin(blkB);
        int pos = 0;
        for (int i = 0; i < 6; i++) {
            txA.setInt(blkA, pos, pos + i + 1, true);
            txB.setInt(blkB, pos, pos + i + 1, true);
            pos = pos + Integer.BYTES;
        }
        txA.setString(blkA, 30, "commitA", true);
        txB.setString(blkB, 30, "commitB", true);

        // 先将 buffer 中的内容写回磁盘, 写的时候，先写日志，再写内容
        logManager.flush(999);
        bufferManager.flushAll(txA.getTxnum());
        bufferManager.flushAll(txB.getTxnum());

        Transaction recoverTx = new Transaction(fileManager, logManager, bufferManager);
        recoverTx.recover();

        // 读取txA的写入的数据
        pos = 0;
        byte[] byteA = new byte[fileManager.getBlockSize()];
        byte[] byteB = new byte[fileManager.getBlockSize()];
        Page pageA = new Page(byteA);
        Page pageB = new Page(byteB);
        fileManager.read(blkA, pageA);
        fileManager.read(blkB, pageB);
        for (int i = 0; i < 6; i++) {
            Integer valA = pageA.getInt(pos);
            Integer valB = pageB.getInt(pos);
            Assert.assertTrue(valA.equals(arr[i]));
            Assert.assertTrue(valB.equals(arr[i]));
            pos = pos + Integer.BYTES;
        }
        Assert.assertEquals(strA, pageA.getString(30));
        Assert.assertEquals(strB, pageB.getString(30));

        // 打印一下日志
        printLog();
    }


}

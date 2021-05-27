package buffer;

import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import server.SimplexDB;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class BufferManagerTest {

    private static FileManager fileManager;
    private static LogManager logManager;

    private static String TEST_DB_DIRECT = "bufferManagerTest";
    private static String TEST_FILE = "bufferManagerTest.dat";
    private static String TEST_LOG_FILE = "bufferManagerTest.log";
    private static File testDirect = null;


    @BeforeClass
    public static void init() {
        testDirect = new File(TEST_DB_DIRECT);
        fileManager = new FileManager(testDirect, SimplexDB.DEFAULT_BLK_SIZE);
        logManager = new LogManager(fileManager, TEST_LOG_FILE);

        File testFile = new File(testDirect, TEST_FILE);
        if (testFile.exists()) {
            testFile.delete();
        }
        File testLogFile = new File(testDirect, TEST_LOG_FILE);
        if (testLogFile.exists()) {
            testLogFile.delete();
        }
    }

    @AfterClass
    public static void finish() {
        File testFile = new File(testDirect, TEST_FILE);
        if (testFile.exists()) {
            testFile.delete();
        }
        File testLogFile = new File(testDirect, TEST_LOG_FILE);
        if (testLogFile.exists()) {
            testLogFile.delete();
        }
        testDirect.delete();

    }

    /**
     * 测试搜索已经分配给相同 block 的 buffer
     */
    @Test
    public void testA1PinFindExistingBuffer() {
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 2);
        BlockId block1 = new BlockId(TEST_FILE, 0);
        Buffer bufferForBlock1 = bufferManager.pin(block1);
        BlockId block2 = new BlockId(TEST_FILE, 1);
        bufferManager.pin(block2);
        Buffer newBuffer = bufferManager.pin(block1);

        Assert.assertEquals(bufferForBlock1, newBuffer);

        bufferManager.unpin(newBuffer);
        newBuffer = bufferManager.pin(block1);
        Assert.assertEquals(bufferForBlock1, newBuffer);
    }

    @Test
    public void testB2PinWaitingTooLangException() {
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 1);
        BlockId block1 = new BlockId(TEST_FILE, 0);
        Buffer bufferForBlock1 = bufferManager.pin(block1);
        BlockId block2 = new BlockId(TEST_FILE, 1);
        Buffer bufferForBlock2 = null;
        try {
            bufferForBlock2 = bufferManager.pin(block2);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof BufferAbortException);
        }
        bufferManager.unpin(bufferForBlock1);
        bufferForBlock2 = bufferManager.pin(block2);

        Assert.assertEquals(bufferForBlock2, bufferForBlock1);
    }

    @Test
    public void testC3PinChooseUnpinned() {
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 2);
        BlockId block1 = new BlockId(TEST_FILE, 0);
        Buffer bufferForBlock1 = bufferManager.pin(block1);
        BlockId block2 = new BlockId(TEST_FILE, 1);
        Buffer bufferForBlock2 = bufferManager.pin(block2);

        Assert.assertNotEquals(bufferForBlock1, bufferForBlock2);

        BlockId blockId3 = new BlockId(TEST_FILE, 2);
        try {
            bufferManager.pin(blockId3);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof BufferAbortException);
        }
    }

    @Test
    public void testD4Unpin() {
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 1);
        BlockId block1 = new BlockId(TEST_FILE, 0);
        Buffer bufferForBlock1 = bufferManager.pin(block1);
        Assert.assertEquals(bufferManager.getAvailableNum(), 0);
        Assert.assertTrue(bufferForBlock1.isPinned());
        // 释放
        bufferManager.unpin(bufferForBlock1);
        Assert.assertEquals(bufferManager.getAvailableNum(), 1);
        Assert.assertFalse(bufferForBlock1.isPinned());
    }

    /**
     * 需要两个线程来测试
     */
    @Test
    public void testE5UnpinNotifyAll() {
        final BufferManager bufferManager = new BufferManager(fileManager, logManager, 1);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread[] threads = new Thread[2];
        for(int i = 0; i < 2; i++) {
            int blockNum = i;
            BlockId blockId = new BlockId(TEST_FILE, blockNum);
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + " has started" + " at " + System.currentTimeMillis());
                    Buffer buffer = bufferManager.pin(blockId);
                    Assert.assertNotNull(buffer);
                    System.out.println(Thread.currentThread().getName() + " has got buffer for blockId " + blockId + " at " + System.currentTimeMillis());
                    try {
                        Thread.sleep(BufferManager.MAX_WAITING_TIME / 2);
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    bufferManager.unpin(buffer);  // 释放，是的另一个线程可以拿到
                    System.out.println(Thread.currentThread().getName() + " has unpinned" + " at " + System.currentTimeMillis());
                    countDownLatch.countDown();
                }
            });
        }

        for(int i = 0; i < 2; i++) {
            threads[i].start();
        }

        // 防止主线程关闭，将子线程也关闭, 将主线程休眠
        try {
            countDownLatch.await();
        }catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testF6FlushAll() {
        BufferManager bufferManager = new BufferManager(fileManager, logManager, 1);
        BlockId block1 = new BlockId(TEST_FILE, 0);
        Buffer bufferForBlock1 = bufferManager.pin(block1);

        int randomOffset = ThreadLocalRandom.current().nextInt(100);
        String testStr = "buffer mannager test for block2";
        Page content = bufferForBlock1.getContent();
        content.setString(randomOffset, testStr);


        // 将其写回磁盘
        int randomTxnum =  ThreadLocalRandom.current().nextInt(100);
        int randomlsn =  ThreadLocalRandom.current().nextInt(100);
        bufferForBlock1.modifyTxnumAndLsn(randomTxnum, randomlsn);  // 一定要先设置 txnum, 否则不会写会磁盘，因为buffer会默认未修复过 content
        bufferManager.flushAll(randomTxnum);

        // 释放缓存，
        bufferManager.unpin(bufferForBlock1);
        // 将缓存分配给两外一个
        BlockId block2 = new BlockId(TEST_FILE, 1);
        Buffer bufferForBlock2 = bufferManager.pin(block2);
        // 在同样的位置写入内容
        String testStr2 = "buffer mannager test for block2";
        Page content2 = bufferForBlock2.getContent();
        content.setString(randomOffset, testStr);
        bufferForBlock1.modifyTxnumAndLsn(randomTxnum + 1, randomlsn + 1);  // 一定要先设置 txnum, 否则不会写会磁盘，因为buffer会默认未修复过 content
        bufferManager.flushAll(randomTxnum);

        bufferManager.unpin(bufferForBlock2);
        bufferForBlock1 = bufferManager.pin(block1);
        content = bufferForBlock1.getContent();
        Assert.assertEquals(content.getString(randomOffset), testStr);


        bufferManager.unpin(bufferForBlock1);
        bufferForBlock2 = bufferManager.pin(block2);
        content2 = bufferForBlock2.getContent();
        Assert.assertEquals(content2.getString(randomOffset), testStr2);
    }


}

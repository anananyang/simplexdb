package buffer;

import file.BlockId;
import file.FileManager;
import file.Page;
import log.LogManager;
import org.junit.*;
import server.SimplexDB;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

public class BufferTest {

    private static FileManager fileManager;
    private static LogManager logManager;
    private static Buffer buffer = null;


    private static String TEST_DB_DIRECT = "bufferTest";
    private static String TEST_FILE = "bufferTest.dat";
    private static String TEST_LOG_FILE = "bufferTest.log";
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

    @Before
    public void setUp() {
        buffer = new Buffer(fileManager, logManager);
    }

    @Test
    public void testAa1PinAndUnpin() {
        buffer.pin();
        Assert.assertEquals(buffer.isPinned(), true);
        buffer.pin();
        Assert.assertEquals(buffer.isPinned(), true);
        buffer.unpin();
        Assert.assertEquals(buffer.isPinned(), true);
        buffer.unpin();
        Assert.assertEquals(buffer.isPinned(), false);
    }


    @Test
    public void testCc3ModifyTxnumAndLsn() {
        int randomTxnum = ThreadLocalRandom.current().nextInt(100);
        int lsn = -2;
        buffer.modifyTxnumAndLsn(randomTxnum, lsn);
        Assert.assertEquals(buffer.getModifyingTx(), randomTxnum);
        Assert.assertNotEquals(buffer.getLsn(), lsn);


        randomTxnum = randomTxnum++;
        lsn = ThreadLocalRandom.current().nextInt(100);
        buffer.modifyTxnumAndLsn(randomTxnum, lsn);
        Assert.assertEquals(buffer.getModifyingTx(), randomTxnum);
        Assert.assertEquals(buffer.getLsn(), lsn);
    }


    @Test
    public void testD4AssignToBlk() {
        // ????????????????????????????????????????????????
        BlockId blockId = new BlockId(TEST_FILE, 0);
        buffer.assignToBlk(blockId);
        Page content = buffer.getContent();
        int randamOffset = ThreadLocalRandom.current().nextInt(100);
        String str = "buff test blockId";
        content.setString(randamOffset, str);
        Assert.assertEquals(buffer.isPinned(), false);


        // ?????????????????????????????????????????????
        buffer.modifyTxnumAndLsn(1, 1);   // ????????????????????? ?????????  buffer.txtnum ????????? 1???????????? buffer ??????????????????????????????
        BlockId newBlockId = new BlockId(TEST_FILE, 1);
        buffer.assignToBlk(newBlockId);
        String str2 = "buff test newBlockId";
        content.setString(randamOffset, str2);
        Assert.assertEquals(buffer.isPinned(), false);


        // ??????????????????????????????????????????????????? str ??????
        buffer.modifyTxnumAndLsn(1, 1);
        buffer.assignToBlk(blockId);
        Assert.assertEquals(content.getString(randamOffset), str);
        Assert.assertEquals(buffer.isPinned(), false);

        // ??????????????????????????????????????????????????? str ??????
        buffer.modifyTxnumAndLsn(1, 1);
        buffer.assignToBlk(newBlockId);
        Assert.assertEquals(content.getString(randamOffset), str2);
        Assert.assertEquals(buffer.isPinned(), false);
    }

}

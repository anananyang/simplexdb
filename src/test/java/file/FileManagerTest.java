package file;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import server.SimplexDB;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 这里的测试需要按照顺序执行，先执行test1WriteRead，再 test2Append
 */
public class FileManagerTest {

    private static FileManager fileManager = null;
    private static File dbDirectory = null;
    private static final String testDirectory = "fileMgrTest";
    private static final String testFileName = "fileTest";
    private static final int readWriteBlockNum = ThreadLocalRandom.current().nextInt(5);

    @BeforeClass
    public static void setUp() {
        dbDirectory = new File(testDirectory);
        fileManager = new FileManager(dbDirectory, SimplexDB.DEFAULT_BLK_SIZE);
        // 如果测试使用的文件已经存在，则将其删除
        File testFile = new File(dbDirectory, testFileName);
        if (testFile.exists()) {
            testFile.delete();
        }
    }

    @AfterClass
    public static void tearDown() {
        File testFile = new File(dbDirectory, testFileName);
        if (testFile.exists()) {
            testFile.delete();
        }
        dbDirectory.delete();
    }


    @Test
    public void testA1WriteRead() {

        BlockId blockId = new BlockId(testFileName, readWriteBlockNum);
        Page writePage = new Page(new byte[SimplexDB.DEFAULT_BLK_SIZE]);

        String str = "simple x db file manager test";
        // 开始测试写
        int offset = ThreadLocalRandom.current().nextInt(100);
        writePage.setString(offset, str);
        fileManager.write(blockId, writePage);
        // 开始测试读
        Page readPage = new Page(new byte[SimplexDB.DEFAULT_BLK_SIZE]);
        fileManager.read(blockId, readPage);

        String readStr = readPage.getString(offset);
        Assert.assertEquals(str, readStr);
    }

    @Test
    public void testB2Append() {
        BlockId blockId = fileManager.append(testFileName);
        Assert.assertNotNull(blockId);
        Assert.assertEquals(blockId.getFileName(), testFileName);
        Assert.assertTrue(blockId.getBlockNum() == (readWriteBlockNum + 1));
    }


}

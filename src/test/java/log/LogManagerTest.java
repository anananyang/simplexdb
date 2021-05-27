package log;

import file.FileManager;
import file.Page;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import server.SimplexDB;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class LogManagerTest {

    private static File directory;
    private static FileManager fileManager;
    private static LogManager logManager;

    private static final String TEST_DB_DIR = "logManagerTest";
    private static final String TEST_DB_LOG_FILE = "testSimplexdb.log";
    private static final String LOG_FORMAT = "simple x db log reoced %d";


    @BeforeClass
    public static void setUp() {
        directory = new File(TEST_DB_DIR);
        File file = new File(directory, TEST_DB_LOG_FILE);
        if (file.exists()) {
            file.delete();
        }

        fileManager = new FileManager(directory, SimplexDB.DEFAULT_BLK_SIZE);
        logManager = new LogManager(fileManager, TEST_DB_LOG_FILE);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(directory, TEST_DB_LOG_FILE);
        if (file.exists()) {
            file.delete();
        }
        directory.delete();
    }

    /**
     * 对日志文件写入，以及使用迭代器读取进行测试，保证写入的内容与读取的内容一致
     */
    @Test
    public void testA1AppendAndInterator() {
        // 生成一个随机数, 作为序列号
        int random = ThreadLocalRandom.current().nextInt(100);
        // 保存序列号对应的日志记录
        Map<Integer, String> logMap = new HashMap<>();
        // 写日志
        for (int i = 0; i < 100; i++) {
            int sn = random + i;
            String logRecord = String.format(LOG_FORMAT, i);
            byte[] bytes = this.createLogRecordByte(logRecord, sn);
            logManager.append(bytes);
            logMap.put(sn, logRecord);
        }

        Iterator<byte[]> it = logManager.iterator();
        while (it.hasNext()) {
            System.out.println("start");
            byte[] bytes = it.next();
            Page page = new Page(bytes);
            String str = page.getString(0);
            System.out.println(str);
            Integer pos = bytes.length - Integer.BYTES;
            int sn = page.getInt(pos);
            // 校验读取的日志是否与写入的一致
            Assert.assertEquals(str, logMap.get(sn));
        }
    }

    /**
     * 生成一条日志记录
     *
     * @param logRecord
     * @param sn
     * @return
     */
    private byte[] createLogRecordByte(String logRecord, int sn) {
        int len = Page.maxLength(logRecord.length());
        int bytesNeeds = len + Integer.BYTES;
        byte[] bytes = new byte[bytesNeeds];
        Page page = new Page(bytes);
        page.setString(0, logRecord);
        page.setInt(len, sn);

        return bytes;
    }

}

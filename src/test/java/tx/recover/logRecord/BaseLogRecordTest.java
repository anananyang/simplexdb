package tx.recover.logRecord;

import file.FileManager;
import log.LogManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import server.SimplexDB;

import java.io.File;

public class BaseLogRecordTest {

    private static final String LOG_RECORD_TEST_DIR = "recoverManagerTest";
    private static final String TEST_LOG_FILE = "logRecordTest.log";

    protected static File dir;
    // 文件管理器
    protected static FileManager fileManager;
    // 日志管理器
    protected static LogManager logManager;



    @BeforeClass
    public static void setUp() {
        dir = new File(LOG_RECORD_TEST_DIR);
        // 判断一下日志文件是否已经存在了，如果已经存在则将其删除
        File file = new File(dir, TEST_LOG_FILE);
        if (file.exists()) {
            file.delete();
        }
        fileManager = new FileManager(dir, SimplexDB.DEFAULT_BLK_SIZE);
        logManager = new LogManager(fileManager, TEST_LOG_FILE);
    }


    @AfterClass
    public static void tearDown() {
        // 测试完成将文件删除
        File file = new File(dir, TEST_LOG_FILE);
        if (file.exists()) {
            file.delete();
        }
        dir.delete();
    }

}

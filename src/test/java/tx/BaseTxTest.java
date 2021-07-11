package tx;

import buffer.BufferManager;
import file.FileManager;
import log.LogIterator;
import log.LogManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import server.SimplexDB;
import tx.recover.logRecord.LogRecord;
import tx.recover.logRecord.LogRecordFactory;

import java.io.File;
import java.util.Iterator;

public class BaseTxTest {

    private static final String TEST_DIR = "txTest";
    private static final String LOG_FILE = "txTest.log";

    protected static File dir;
    // 文件管理器
    protected static FileManager fileManager;
    // 日志管理器
    protected static LogManager logManager;
    // 缓存管理器
    protected static BufferManager bufferManager;




    @BeforeClass
    public static void setUp() {
        dir = new File(TEST_DIR);
        // 判断一下日志文件是否已经存在了，如果已经存在则将其删除
        File file = new File(dir, LOG_FILE);
        if (file.exists()) {
            file.delete();
        }
        fileManager = new FileManager(dir, SimplexDB.DEFAULT_BLK_SIZE);
        logManager = new LogManager(fileManager, LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, SimplexDB.DEFAULT_BUFFER_SIZE);
    }


    @AfterClass
    public static void tearDown() {
        // 测试完成将文件删除
        removeDir(dir);
    }

    private static void removeDir(File dir) {
        for(File file : dir.listFiles()) {
            if(file.isDirectory()) {
                removeDir(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }



    protected void printLog() {
        System.out.println("------  start to print log -----");
        Iterator<byte[]> it = logManager.iterator();
        while(it.hasNext()) {
            byte[] bytes = it.next();
            LogRecord logRecord = LogRecordFactory.createLogRecord(bytes);
            System.out.println(logRecord);
        }

    }

}

package server;

import buffer.BufferManager;
import file.FileManager;
import log.LogManager;
import tx.Transaction;

import java.io.File;

public class SimplexDB {

    // 默认块大小
    public final static Integer DEFAULT_BLK_SIZE = 400;
    // 默认缓冲池大小
    public final static int DEFAULT_BUFFER_SIZE = 8;
    // 默认日志文件
    public final static String LOG_FILE = "simplexdb.log";
    // 默认数据库
    public final static String DEFAULT_DB = "defaultDB";

    private FileManager fileManager;
    private LogManager logManager;
    private BufferManager bufferManager;

    private int blockSize = DEFAULT_BLK_SIZE;
    private String curDirectory;

    public SimplexDB(String directory, Integer blockSize) {
        this.curDirectory = directory;
        if (blockSize != null) {
            this.blockSize = Math.max(blockSize, DEFAULT_BLK_SIZE);
        }
        File dbDirectory = new File(directory);
        fileManager = new FileManager(dbDirectory, blockSize);
        logManager = new LogManager(fileManager, LOG_FILE);
        bufferManager = new BufferManager(fileManager, logManager, DEFAULT_BUFFER_SIZE);
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public BufferManager getBufferManager() {
        return bufferManager;
    }

    public Transaction newTx() {
        return new Transaction(fileManager, logManager, bufferManager);
    }
}

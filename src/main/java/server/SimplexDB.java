package server;

import file.FileManager;
import log.LogManager;

import java.io.File;

public class SimplexDB {

    public static int DEFAULT_BLK_SIZE = 400;
    public static String LOG_FILE = "simplexdb.log";
    public static String DEFAULT_DB = "defaultDB";

    private FileManager fileManager;
    private LogManager logManager;

    private int blockSize = DEFAULT_BLK_SIZE;
    private String curDirectory;

    public SimplexDB(String directory, Integer blockSize) {
        this.curDirectory = directory;
        if(blockSize != null) {
            this.blockSize = Math.max(blockSize, DEFAULT_BLK_SIZE);
        }
        File dbDirectory = new File(directory);
        fileManager = new FileManager(dbDirectory, blockSize);
        logManager = new LogManager(fileManager, LOG_FILE);
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public LogManager getLogManager() {
        return logManager;
    }
}

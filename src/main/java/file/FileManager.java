package file;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件管理器的最重要的两个功能
 * 1. 将文件的某个部分(blockId) 写入到 Page 中
 * 2. 将 Page 中缓存的内容写入到文件的指定位置（具体位置根据 blockId 进行计算）
 */
public class FileManager {
    // 数据库所在目录
    private File dbDirectory;
    // 块大小
    private int blockSize;
    // 判断目录之前是否存在，isNew = true 表示目录之前不存在
    private boolean isNew;
    private Map<String, RandomAccessFile> openedFiles = new HashMap<>();


    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        this.isNew = !dbDirectory.exists();
        if (this.isNew) {
            dbDirectory.mkdirs();   // 如果目录不存在则创建
        }

        // 清空临时文件，TODO 如果isNew = true，是否还需要清空
        for (String fileName : dbDirectory.list()) {
            if (fileName.startsWith("temp")) {
                new File(fileName).delete();
            }
        }
    }

    public Boolean isNew() {
        return this.isNew;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public File getDbDirectory() {
        return dbDirectory;
    }

    /**
     * 将指定块读取到 page 中
     *
     * @param blk
     * @param page
     */
    public synchronized void read(BlockId blk, Page page) {
        try {
            RandomAccessFile file = getFile(blk.getFileName());
            file.seek(blk.getBlockNum() * blockSize);
            file.getChannel().read(page.getContent());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * 将指定 Page 写到文件的 blk 的位置
     *
     * @param blk
     * @param page
     */
    public synchronized void write(BlockId blk, Page page) {
        try {
            RandomAccessFile file = getFile(blk.getFileName());
            file.seek(blk.getBlockNum() * blockSize);
            file.getChannel().write(page.getContent());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block " + blk);
        }
    }

    /**
     * 给指定的文件新加一个块
     * 1、计算新的块编号
     * 2、将新的块对应的文件位置全部重写为0（java new 一个对象时，在 new 成功之后将相应的内存区域清零）。请零最大的好处就是方便计算 blockNum
     *
     * @param fileName
     * @return
     */
    public synchronized BlockId append(String fileName) {
        int blockNum = this.getNewBlockNum(fileName);
        BlockId blockId = new BlockId(fileName, blockNum);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile file = this.getFile(fileName);
            file.seek(blockId.getBlockNum() * blockSize);
            file.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + blockNum);
        }

        return blockId;
    }


    /**
     * 计算指定的文件的新的 block 的 number
     * ps: 因为在 append 一个newBlock时，会将newBlock对应的文件位置 BlockSize 大小都重写为 0
     * 所以被 FileManager 所管理的文件长度一定是 blockSize 的整数倍
     *
     * @param fileName
     * @return
     */
    public int getNewBlockNum(String fileName) {
        try {
            RandomAccessFile file = this.getFile(fileName);
            long fileLen = file.length();
            return (int) (fileLen / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot get new block num of file [ " + fileName + " ]");
        }

    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile randomAccessFile = openedFiles.get(fileName);
        if (randomAccessFile == null) {
            File dbTable = new File(dbDirectory, fileName);
            randomAccessFile = new RandomAccessFile(dbTable, "rws");
            openedFiles.put(fileName, randomAccessFile);
        }
        return randomAccessFile;
    }


}

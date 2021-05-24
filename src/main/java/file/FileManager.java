package file;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

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
     * 2、将新的块对应的文件位置全部 reset 为 0
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
     *
     * @param fileName
     * @return
     */
    private int getNewBlockNum(String fileName) {
        try {
            RandomAccessFile file = this.getFile(fileName);
            long fileLen = file.length();
            return (int) (fileLen / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot get new block number of file [ " + fileName + " ]");
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

package log;

import file.BlockId;
import file.FileManager;
import file.Page;

import java.util.Iterator;


/**
 * logFile 的迭代器从后向前开始迭代
 */
public class LogIterator implements Iterator<byte[]> {

    private FileManager fileManager;
    private BlockId blockId;
    private Page logPage;
    private Integer curPosition;

    public LogIterator(FileManager fileManager, BlockId blockId) {
        this.fileManager = fileManager;
        this.blockId = blockId;

        // 读取记录时，所有的块都共用一个 logPage
        byte[] bytes = new byte[fileManager.getBlockSize()];
        logPage = new Page(bytes);
        // 读取logFile最后一个块的内容，记录 boundary 的位置，读取时从 boundary 的位置开始读取
        this.moveToBlock();
    }

    /**
     * 只有在两种情况下表示还有可以读取的日志记录
     * 1. 当前 logPage 还没有读取到边界位置
     * 2. 其实当前块不是第一个块。因为日志记录的读取是从最后一个块向前开始读取的
     *
     * @return
     */
    @Override
    public boolean hasNext() {
        return curPosition < fileManager.getBlockSize() || blockId.getBlockNum() > 0;
    }

    /**
     * 读取一个条记录
     * @return
     */
    @Override
    public byte[] next() {
        // 如果当前已经读取到边界
        if(curPosition == fileManager.getBlockSize()) {
            blockId = new BlockId(blockId.getFileName(), blockId.getBlockNum() - 1);
            moveToBlock();
        }
        byte[] bytes = logPage.getBytes(curPosition);
        // 计算下一条记录的位置
        curPosition = curPosition + bytes.length + Integer.BYTES;

        return bytes;
    }


    private void moveToBlock() {
        fileManager.read(blockId, logPage);
        int boundary = logPage.getInt(0);
        this.curPosition = boundary;
    }
}

package file;

/**
 * simplexDB会将文件按照指定大小（默认400）的字节进行分块，BlockId 用于描述文件的指定块。
 */
public class BlockId {
    private String fileName;
    private int blockNum;  // 从 0 开始计数


    public BlockId(String fileName, int blockNum) {
        this.fileName = fileName;
        this.blockNum = blockNum;
    }

    public String getFileName() {
        return fileName;
    }

    public Integer getBlockNum() {
        return blockNum;
    }

    public String toString() {
        return "[fileName " + fileName + ", blockNum " + blockNum + "]";
    }

    @Override
    public boolean equals(Object o) {
        // buffer pin 时，如果 buffer 还未分配过，o 会为 null
        if(o == null) {
            return false;
        }
        BlockId block = (BlockId) o;
        return block.getFileName().equals(fileName) && block.getBlockNum().equals(blockNum);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}



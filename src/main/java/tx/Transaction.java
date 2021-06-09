package tx;

import buffer.Buffer;
import file.BlockId;
import file.FileManager;

/**
 * 事务主要是保证ACID性质
 * 1. 原子性
 * 2. 一致性
 * 3. 隔离性
 * 4. 持久性
 * 其中事务利用恢复管理器写log来保证事务都原子性和持久性；利用并发管理器来保证事务都隔离性和一致性
 * <p>
 * 可以将 Transaction 的方法分为三个部分
 * 1. 与 recoverManager 相关的操作
 * 2. 与 buffer 相关的操作
 * 3. 与 fileManager 相关的操作
 */
public class Transaction {

    private static int nextTxnum = -1;
    private int txnum;
    private FileManager fileManager;

    public int getTxnum() {
        return txnum;
    }

    public void commit() {
    }

    public void rollback() {
    }

    public void recover() {
    }


    public Buffer pin(BlockId blockId) {
        return null;
    }

    public void unpin(BlockId blockId) {
    }

    public int getInt(BlockId blockId, int offset) {
        return 0;
    }

    public void setInt(BlockId blockId, int offset, int value, boolean okToLog) {
    }

    public String getString(BlockId blockId, int offset) {
        return "";
    }

    public void setString(BlockId blockId, int offset, String str, boolean okToLog) {
    }

    public int availableBuffs() {
        return 0;
    }

    public int size() {
        return 0;
    }

    public BlockId append(String fileName) {
        return fileManager.append(fileName);
    }

    public int blockSize() {
        return fileManager.getBlockSize();
    }

    // 生成事务ID的静态方法, 需要同步获取
    private static synchronized int getNextTxnum() {
        nextTxnum = nextTxnum + 1;
        return nextTxnum;
    }


}

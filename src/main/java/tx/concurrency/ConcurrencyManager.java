package tx.concurrency;

import file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * 管理某个事务所获取的锁
 */
public class ConcurrencyManager {
    // 全局的 lockTable，记录全局的锁
    private static LockTable lockTable = new LockTable();
    // 记录当前事务获取到的锁
    private Map<BlockId, String> locks = new HashMap<>();

    /**
     * 为 blk 获取一个共享锁
     *
     * @param blk
     */
    public void sLock(BlockId blk) {
        /**
         * 这里判断 blk 有没有锁
         * Note：判断 blk 有没有锁时，并没有指定锁的类型是 sLock，这样中的原因是 blk 具有 xLock，
         * 那么该 blk 也有 sLock
         */
        if (!locks.containsKey(blk)) {
            lockTable.sLock(blk);
            locks.put(blk, LockType.sLock);
            System.out.println("thread [ " + Thread.currentThread().getName() + " ] get sLock for " + blk +
                    " at " + System.nanoTime());
        }
    }

    /**
     * 为 blk 获取一个独占锁
     *
     * @param blk
     */
    public void xLock(BlockId blk) {
        // 已经有锁
        if (hasXLock(blk)) {
            return;
        }
        // 先获取共享锁
        this.sLock(blk);
        // 再获取独占锁
        lockTable.xLock(blk);
        // 记录锁
        locks.put(blk, LockType.xLock);

        System.out.println("thread [ " + Thread.currentThread().getName() + " ] get xLock for " + blk +
                " at " + System.nanoTime());
    }

    /**
     * 释放所有的锁
     */
    public void release() {
        for (BlockId blk : locks.keySet()) {
            lockTable.unlock(blk);
        }
        locks.clear();
    }

    public Boolean hasXLock(BlockId blk) {
        return LockType.xLock.equals(locks.get(blk));
    }

    public String getLockType(BlockId blk) {
        return locks.get(blk);
    }
}

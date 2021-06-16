package tx.concurrency;

import file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * 管理全局的锁
 */
public class LockTable {

    // 最终等待10秒
    private final static Long MAX_WAITTING_TIME = 10000l;
    /**
     * 维护全局的锁
     * value = -1 表示独占锁
     * value > 0 表示共享锁
     */
    private Map<BlockId, Integer> locks = new HashMap<>();

    /**
     * 只要指定 blk 上没有独占锁(xLock)，就可以获取 sLock
     *
     * @param blk
     */
    public synchronized void sLock(BlockId blk) {
        try {
            Long startTime = System.currentTimeMillis();
            while (hasXLock(blk) && !waittingTooLong(startTime)) {
                wait(MAX_WAITTING_TIME);
            }
            // 如果是等待时间太长导致的未获取到锁
            if (hasXLock(blk)) {
                throw new LockAbortException();
            }
            Integer value = getLockValue(blk);
            value = value + 1;
            locks.put(blk, value);
        } catch (Exception e) {
            throw new LockAbortException();
        }

    }

    public synchronized void xLock(BlockId blk) {
        try {
            Long startTime = System.currentTimeMillis();
            while (hasOtherSLock(blk) && !waittingTooLong(startTime)) {
                wait(MAX_WAITTING_TIME);
            }
            // 如果是等待时间太长导致的未获取到锁
            if (hasOtherSLock(blk)) {
                throw new LockAbortException();
            }
            locks.put(blk, -1);
        } catch (Exception e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId blk) {
        Integer val = getLockValue(blk);
        // 如果除了当前事务之外，还有其他事务持有该共享锁
        if(val > 1) {
            locks.put(blk, val);
        } else {
            locks.remove(blk);  // 释放锁
            notifyAll();        // 释放锁之后需要唤醒可能在等待锁的事务去竞争锁
        }
    }

    /**
     * 判断指定 blk 是否加了独占锁
     *
     * @param blk
     * @return
     */
    private Boolean hasXLock(BlockId blk) {
        return getLockValue(blk) < 0;
    }

    /**
     * 判断指定 blk 是否加了超过1个共享锁
     *
     * @param blk
     * @return
     */
    private Boolean hasOtherSLock(BlockId blk) {
        return getLockValue(blk) > 1;
    }

    private Integer getLockValue(BlockId blk) {
        Integer val = locks.get(blk);
        return val == null ? 0 : val;
    }

    private Boolean waittingTooLong(Long startTime) {
        return System.currentTimeMillis() - startTime > MAX_WAITTING_TIME;
    }

}

package tx.concurrency;

import file.BlockId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class LockTableTest {

    private LockTable lockTable;

    @Before
    public void init() {
        lockTable = new LockTable();
    }

    @Test
    public void testA1SLock() {
        BlockId blockId = new BlockId("lockTableTest", 0);
        lockTable.xLock(blockId);
        try {
            lockTable.sLock(blockId);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof LockAbortException);
        }
    }

    @Test
    public void testB2XLock() {
        BlockId blockId = new BlockId("lockTableTest", 0);
        lockTable.sLock(blockId);
        lockTable.sLock(blockId);
        try {
            lockTable.xLock(blockId);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof LockAbortException);
        }
    }

    @Test
    public void testC3Unlock() {
        BlockId blockId = new BlockId("lockTableTest", 0);
        lockTable.sLock(blockId);
        lockTable.sLock(blockId);
        try {
            lockTable.xLock(blockId);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof LockAbortException);
        }
        lockTable.unlock(blockId);
        lockTable.xLock(blockId);
        lockTable.unlock(blockId);
        Assert.assertTrue(lockTable.getLockNum() == 0);
    }

}

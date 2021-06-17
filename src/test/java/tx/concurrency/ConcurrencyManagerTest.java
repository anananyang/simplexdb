package tx.concurrency;

import file.BlockId;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ConcurrencyManagerTest {


    @Test
    public void testA1SLock() {
        ConcurrencyManager concurrencyMgr1 = new ConcurrencyManager();
        ConcurrencyManager concurrencyMgr2 = new ConcurrencyManager();

        // 两个都加 sLock
        BlockId blockId = new BlockId("concurrencyMgr", 1);
        concurrencyMgr1.sLock(blockId);
        Assert.assertEquals(LockType.sLock, concurrencyMgr1.getLockType(blockId));
        concurrencyMgr2.sLock(blockId);
        Assert.assertEquals(LockType.sLock, concurrencyMgr2.getLockType(blockId));

        // 一个先加 xLock，另一个再加 sLock
        BlockId blockId2 = new BlockId("concurrencyMgr", 2);
        concurrencyMgr1.xLock(blockId2);
        Assert.assertEquals(LockType.xLock, concurrencyMgr1.getLockType(blockId2));
        try {
            concurrencyMgr2.sLock(blockId2);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LockAbortException);
        }
        concurrencyMgr1.release();
        concurrencyMgr2.sLock(blockId2);
        Assert.assertEquals(LockType.sLock, concurrencyMgr2.getLockType(blockId2));


        concurrencyMgr1.release();
        concurrencyMgr2.release();
    }

    @Test
    public void testB2XLock() {
        ConcurrencyManager concurrencyMgr1 = new ConcurrencyManager();
        ConcurrencyManager concurrencyMgr2 = new ConcurrencyManager();

        // 两个都加 xLock
        BlockId blockId = new BlockId("concurrencyMgr", 1);
        concurrencyMgr1.xLock(blockId);
        Assert.assertEquals(LockType.xLock, concurrencyMgr1.getLockType(blockId));
        try {
            concurrencyMgr2.xLock(blockId);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LockAbortException);
        }
        concurrencyMgr1.release();
        concurrencyMgr2.xLock(blockId);
        Assert.assertEquals(LockType.xLock, concurrencyMgr2.getLockType(blockId));
    }


    /**
     * 多线程模拟竞争锁
     */
    @Test
    public void TestC3ThreadGetLock() {
//        Long startNanoTime = System.nanoTime()
        // 只要超过2个线程在同时竞争同一个 xLock 可能会取不到锁（在2个线程都取到sLock准备获取xLock时，此时发生死锁）
        int count = 2;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        BlockId blockId = new BlockId("concurrencyMgr", 1);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    ConcurrencyManager concurrencyManager = new ConcurrencyManager();
                    Integer randomVal = ThreadLocalRandom.current().nextInt(100);
                    System.out.println("thread [ " + Thread.currentThread().getName() + " ] get randomVal: " + randomVal);
                    if (randomVal > 50) {
                        System.out.println("thread [ " + Thread.currentThread().getName() + " ] is going to get sLock at " + System.nanoTime());
                        concurrencyManager.sLock(blockId);
                    } else {
                        System.out.println("thread [ " + Thread.currentThread().getName() + " ] is going to get xLock at " + System.nanoTime());
                        concurrencyManager.xLock(blockId);
                    }
                    Long sleepTime = ThreadLocalRandom.current().nextLong(10000);
                    System.out.println("thread [ " + Thread.currentThread().getName() + " ] is going to sleep " + sleepTime + " millis at " + System.nanoTime());
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("thread [ " + Thread.currentThread().getName() + " ] unlocked at " + System.nanoTime());
                    concurrencyManager.release();
                } catch (Exception e) {
                    throw e;
                } finally {
                    countDownLatch.countDown();
                }


            }
        };
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(runnable);
        }
        for (int i = 0; i < count; i++) {
            threads[i].start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("thread [ " + Thread.currentThread().getName() + " ] finished at " + System.nanoTime());
    }

}

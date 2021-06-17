package tx;

import buffer.Buffer;
import file.BlockId;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class BufferListTest extends BaseTxTest{

    @Test
    public void testA1Pin() {
        BufferList bufferList = new BufferList(bufferManager);
        BlockId blkA = new BlockId("bufferListTest", 0);
        bufferList.pin(blkA);
        Assert.assertTrue(1 == bufferList.getBufferNum());
        bufferList.pin(blkA);
        Assert.assertTrue(1 == bufferList.getBufferNum());

        Buffer buffer = bufferList.getBuffer(blkA);
        Assert.assertTrue(1 == buffer.getPin());

        BlockId blkB = new BlockId("bufferListTest", 1);
        bufferList.pin(blkB);
        Assert.assertTrue(2 == bufferList.getBufferNum());
    }

    @Test
    public void testB2Unpin() {
        BufferList bufferList = new BufferList(bufferManager);
        BlockId blkA = new BlockId("bufferListTest", 0);
        bufferList.pin(blkA);
        Assert.assertTrue(1 == bufferList.getBufferNum());
        bufferList.unpin(blkA);
        Assert.assertTrue(0 == bufferList.getBufferNum());
    }

    @Test
    public void testC3UnpinAll() {
        BufferList bufferList = new BufferList(bufferManager);
        BlockId blkA = new BlockId("bufferListTest", 0);
        BlockId blkB = new BlockId("bufferListTest", 1);
        BlockId blkC = new BlockId("bufferListTest", 2);
        bufferList.pin(blkA);
        bufferList.pin(blkB);
        bufferList.pin(blkC);
        Assert.assertTrue(3 == bufferList.getBufferNum());
        bufferList.unpin(blkA);
        Assert.assertTrue(2 == bufferList.getBufferNum());
        bufferList.unpinAll();
        Assert.assertTrue(0 == bufferList.getBufferNum());
    }
}

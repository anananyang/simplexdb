package file;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockIdTest {

    private BlockId blockId = null;

    @Before
    public void setUp() {
        blockId = new BlockId("test", 1);
    }

    @Test
    public void getFileNameTest() {
        Assert.assertEquals("test", blockId.getFileName());
    }

    @Test
    public void getBlockNumTest() {
        Assert.assertTrue( blockId.getBlockNum() == 1);
    }

    @Test
    public void blockEqualTest() {
        BlockId blockIdT1 = new BlockId("test1", 1);
        Assert.assertNotEquals(blockIdT1, blockId);

        BlockId blockIdT2 = new BlockId("test", 0);
        Assert.assertNotEquals(blockIdT2, blockId);

        BlockId blockIdT3 = new BlockId("test", 1);
        Assert.assertEquals(blockIdT3, blockId);
    }

}

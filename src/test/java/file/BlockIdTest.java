package file;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockIdTest {

    private static BlockId blockId = null;

    @BeforeClass
    public static void setUp() {
        blockId = new BlockId("test", 1);
    }

    @Test
    public void getFileNameTest1() {
        Assert.assertEquals("test", blockId.getFileName());
    }

    @Test
    public void getBlockNumTest2() {
        Assert.assertTrue( blockId.getBlockNum() == 1);
    }

    @Test
    public void blockEqualTest3() {
        BlockId blockIdT1 = new BlockId("test1", 1);
        Assert.assertNotEquals(blockIdT1, blockId);

        BlockId blockIdT2 = new BlockId("test", 0);
        Assert.assertNotEquals(blockIdT2, blockId);

        BlockId blockIdT3 = new BlockId("test", 1);
        Assert.assertEquals(blockIdT3, blockId);
    }

}

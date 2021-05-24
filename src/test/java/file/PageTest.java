package file;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import server.SimplexDB;

import java.util.concurrent.ThreadLocalRandom;

public class PageTest {

    private Page directByteBufferPage = null;
    private Page byteArrayWrapPage = null;


    @Before
    public void setUp() {
        int blockSize = SimplexDB.DEFAULT_BLK_SIZE;
        directByteBufferPage = new Page(blockSize);
        byteArrayWrapPage = new Page(new byte[blockSize]);
    }

    @Test
    public void getSetIntTest() {
       int len = SimplexDB.DEFAULT_BLK_SIZE / Integer.BYTES;
       for(int i = 0; i < len; i++) {
           int offset = i * Integer.BYTES;
           byteArrayWrapPage.setInt(offset, i);
           directByteBufferPage.setInt(offset, i);
       }

        for(int i = 0; i < len; i++) {
            int offset = i * Integer.BYTES;
            Assert.assertTrue(byteArrayWrapPage.getInt(offset) == i);
            Assert.assertTrue(directByteBufferPage.getInt(offset) == i);
        }
    }

    public void getSetStringTest() {
        int offset = ThreadLocalRandom.current().nextInt(100);
        String str = "anyanggogogo";
        byteArrayWrapPage.setString(offset, str);
        directByteBufferPage.setString(offset, str);

        Assert.assertEquals(str, byteArrayWrapPage.getString(offset));
        Assert.assertEquals(str, directByteBufferPage.getString(offset));
    }

}

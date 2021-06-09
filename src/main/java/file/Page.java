package file;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class Page {
    private ByteBuffer byteBuffer;
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * 在jvm堆上分配的存储空间
     *
     * @param bytes
     */
    public Page(byte[] bytes) {
        this.byteBuffer = ByteBuffer.wrap(bytes);
    }

    /**
     * 在jvm堆外内存分配的存储空间，在堆外分配的好处是可以加速与外设之间的内存访问
     *
     * @param blockSize
     */
    public Page(int blockSize) {
        this.byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * 计算字符串 str 会占用的长度
     *
     * @param strLen 字符串长度
     * @return
     */
    public static int maxLength(int strLen) {
        // 每个字符所占的字节数
        int bytesPerChar = (int) CHARSET.newEncoder().maxBytesPerChar();
        // 字符串所占用的长度
        return Integer.BYTES + (strLen * bytesPerChar);
    }

    /**
     * 在 ByteBuffer 的指定位置读取一个 int 类型的数字
     *
     * @param offset
     * @return
     */
    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    /**
     * 在 ByteBuffer 的指定位置写一个 int 类型的数字
     *
     * @param offset
     * @param n
     */
    public void setInt(int offset, int n) {
        byteBuffer.putInt(offset, n);
    }

    /**
     * 在指定位置读取读取一定长度的字节，字节长度由在 offset 位置的 int 类型的数字表示
     *
     * @param offset
     */
    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int len = byteBuffer.getInt();
        byte[] bytes = new byte[len];
        byteBuffer.get(bytes);

        return bytes;
    }

    /**
     * 在指定位置写入一定长度的字节，为了取的时候方便，
     * 1、先写入字节的长度
     * 2、再写入字节
     *
     * @param offset
     * @param bytes
     */
    public void setBytes(int offset, byte[] bytes) {
        // jdk 版本过高，会报 NoSuchMethod 异常
        ((Buffer)byteBuffer).position(offset);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }

    /**
     * 写入字符串。
     * 1、先将字符串按照指定的编码转换成字节数组
     * 2、将字节数组写入到 ByteBuffer 中
     *
     * @param offset
     * @param str
     */
    public void setString(int offset, String str) {
        byte[] bytes = str.getBytes(CHARSET);
        setBytes(offset, bytes);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    /**
     * 该方法主要由 fileManager 使用. fileManager 获取到 ByteBuffer 之后
     * 可以将 ByteBuffer 的内容写入到指定的 block 中，或者将指定的 block 读取到 ByteBuffer 中
     *
     * @return
     */
    ByteBuffer getContent() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}

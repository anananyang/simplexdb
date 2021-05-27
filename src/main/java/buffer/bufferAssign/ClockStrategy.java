package buffer.bufferAssign;

import buffer.Buffer;
import file.FileManager;
import log.LogManager;
import util.ArrayIterator;

import java.util.Iterator;

/**
 * 每次分配buffer时，都从上次被分配的 buffer 的下一个buffer开始遍历
 */
public class ClockStrategy implements BufferAssignStrategy {

    private Buffer[] buffers;
    private int bufferSize = -1;
    // 最近一次被分配的缓存在 buffers 中的下标 + 1, 表示下次遍历的开始的位置
    private int startIndex = 0;

    public ClockStrategy(FileManager fileManager, LogManager logManager, int bufferSize) {
        this.bufferSize = bufferSize;
        this.buffers = new Buffer[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            buffers[i] = new Buffer(fileManager, logManager);
        }
    }

    @Override
    public Iterator<Buffer> iterator() {
        return new ArrayIterator<Buffer>(buffers);
    }

    @Override
    public Buffer chooseUnpinnedBuffer() {
        Buffer buffer = null;
        int index = -1;
        for(int i = 0 ;i < bufferSize; i++) {
            index = (startIndex + i) % bufferSize;
            buffer = buffers[index];
            if(!buffer.isPinned()) {
               break;
            }
        }
        if(buffer != null) {
            this.startIndex = index + 1;  // 下一次开始遍历的位置时本次分配的数组元素的下一个元素的位置
        }
        return buffer;
    }

    @Override
    public void unpin(Buffer buffer) {
        // do nothing
    }
}


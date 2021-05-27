package buffer.bufferAssign;

import buffer.Buffer;
import file.FileManager;
import log.LogManager;
import util.ArrayIterator;

import java.util.Iterator;

/**
 * 最简单的可用缓存分配策略
 */
public class NaiveStrategy implements BufferAssignStrategy {

    private Buffer[] buffers;


    public NaiveStrategy(FileManager fileManager, LogManager logManager, int bufferSize) {
        buffers = new Buffer[bufferSize];
        for(int i = 0; i < bufferSize; i++) {
            buffers[i] = new Buffer(fileManager, logManager);
        }
    }

    @Override
    public Iterator<Buffer> iterator() {
        return new ArrayIterator<Buffer>(buffers);
    }

    @Override
    public Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer : buffers) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    @Override
    public void unpin(Buffer buffer) {
        // do nothing
    }
}
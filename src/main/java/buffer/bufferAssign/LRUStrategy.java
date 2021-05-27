package buffer.bufferAssign;

import buffer.Buffer;
import file.FileManager;
import log.LogManager;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 先被 unpin 的 buffer 优先分配
 */
public class LRUStrategy implements BufferAssignStrategy {

    private LinkedList<Buffer> bufferList;

    public LRUStrategy(FileManager fileManager, LogManager logManager, int bufferSize) {
        bufferList = new LinkedList();
        for (int i = 0; i < bufferSize; i++) {
            bufferList.addFirst(new Buffer(fileManager, logManager));
        }
    }


    @Override
    public Iterator<Buffer> iterator() {
        return bufferList.iterator();
    }

    @Override
    public Buffer chooseUnpinnedBuffer() {
        Iterator<Buffer> it = bufferList.iterator();
        Buffer buffer = null;
        while (it.hasNext()) {
            buffer = it.next();
            if (!buffer.isPinned()) {
                break;
            }
        }
        return buffer;
    }

    /**
     * 将 unpin 的 buffer 从 buffer链表中删除，并添加到链表末尾
     *
     * @param buffer
     */
    @Override
    public void unpin(Buffer buffer) {
        bufferList.remove(buffer);
        bufferList.addLast(buffer);
    }

}

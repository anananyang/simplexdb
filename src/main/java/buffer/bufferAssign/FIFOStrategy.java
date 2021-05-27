package buffer.bufferAssign;

import buffer.Buffer;
import file.FileManager;
import log.LogManager;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * 先被 pin 的 buffer（当前是 unpin 状态）优先分配
 */
public class FIFOStrategy implements BufferAssignStrategy {

    private LinkedList<Buffer> bufferList;

    public FIFOStrategy(FileManager fileManager, LogManager logManager, int bufferSize) {
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
        while(it.hasNext()) {
            buffer = it.next();
            if(!buffer.isPinned()) {
                it.remove();   // 将其从链表中删除
                break;
            }
        }
        if(buffer != null) {
            bufferList.addLast(buffer);   // 将刚分配的缓存添加到最后
        }
        return buffer;
    }

    @Override
    public void unpin(Buffer buffer) {
        // do nothing
    }
}

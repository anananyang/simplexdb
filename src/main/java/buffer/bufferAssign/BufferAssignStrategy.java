package buffer.bufferAssign;

import buffer.Buffer;
import file.BlockId;

import java.util.Iterator;

/**
 * 缓存的分配策略
 */
public interface BufferAssignStrategy {

    Iterator<Buffer> iterator();

    Buffer chooseUnpinnedBuffer();

    void unpin(Buffer buffer);

}

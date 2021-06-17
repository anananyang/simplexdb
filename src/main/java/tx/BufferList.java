package tx;

import buffer.Buffer;
import buffer.BufferManager;
import file.BlockId;
import util.ListUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 记录当前事务所获取到到所有的buffer
 */
public class BufferList {

    private BufferManager bufferManager;
    // 记录当前事务获取的所有 blk
    private List<BlockId> pins = new ArrayList<>();
    // 记录 BlockId 对应的 buffer，主要用于事务 unpin 时，根据 blk 取出 buffer
    private Map<BlockId, Buffer> bufferMap = new HashMap<>();

    public BufferList(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    /**
     * 给指定 blk 获取一个 buffer
     *
     * @param blk
     * @return
     */
    void pin(BlockId blk) {
        // 已经获取过buffer
        if(bufferMap.containsKey(blk)) {
            return;
        }
        Buffer buffer = bufferManager.pin(blk);
        pins.add(blk);
        bufferMap.put(blk, buffer);
    }

    /**
     * 释放指定的 blk 的 buffer
     *
     * @param blk
     */
    void unpin(BlockId blk) {
        pins.remove(blk);
        Buffer buffer = bufferMap.remove(blk);
        if (buffer == null) {
            return;
        }
        buffer.unpin();
    }

    /**
     * 释放所有获取到到 buffer
     */
    void unpinAll() {
        if (ListUtil.isBlank(pins)) {
            return;
        }
        for (BlockId blk : pins) {
            Buffer buffer = bufferMap.get(blk);
            if (buffer != null) {
                bufferManager.unpin(buffer);
            }
        }
        bufferMap.clear();
        pins.clear();
    }

    Buffer getBuffer(BlockId blk) {
        return bufferMap.get(blk);
    }

    Integer getBufferNum() {
        return ListUtil.isBlank(pins) ? 0 : pins.size();
    }

}

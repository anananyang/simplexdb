package record;


import file.BlockId;
import tx.Transaction;

import java.sql.Types;

/**
 * 对block指定位置进行读写
 */
public class RecordPage {
    // 标记位
    private static final int USED = 1, EMPTY = 0;
    // 当前 recordPage 对应的 blk
    private BlockId blk;
    // 使用 tx 进行读写
    private Transaction tx;
    // record 的信息
    private Layout layout;


    public RecordPage(Transaction tx, BlockId blk, Layout layout) {
        this.tx = tx;
        this.blk = blk;
        this.layout = layout;
        // 在读写 blk 前，先将
        tx.pin(blk);
    }

    public BlockId getBlk() {
        return blk;
    }

    /**
     * 将当前块进行格式化
     */
    public void format() {
        // 从第一个 slot 开始格式化
        int slot = 0;
        while(isValidSlot(slot)) {
            // 先将标记位设置为 EMPTY
            setFlag(slot, EMPTY);
            // 再初始化 content of curret flot
            Schema schema = layout.getSchema();
            for(String field : schema.getFieldNameList()) {
                int offset = layout.getFieldOffset(field);
                int type = schema.getType(field);
                if(type == Types.INTEGER) {
                    tx.setInt(blk, offset, 0, false);
                }
                else if (type == Types.VARCHAR) {
                    tx.setString(blk, offset, "", false);
                }
                slot++;
            }
        }
    }

    /********* 改 ××××××××××××/

    /**
     * 读取 slot 记录的字段的值
     *
     * NOTE: slot 从 0 开始计数
     * @param slot
     * @param field
     * @return
     */
    public int getInt(int slot, String field) {
        int filedOffset = offset(slot) + layout.getFieldOffset(field);
        return tx.getInt(blk, filedOffset);
    }

    public void setInt(int slot, String field, int val) {
        int filedOffset = offset(slot) + layout.getFieldOffset(field);
        tx.setInt(blk, filedOffset, val, true);
    }

    public String getString(int slot, String field) {
        int filedOffset = offset(slot) + layout.getFieldOffset(field);
        return tx.getString(blk, filedOffset);
    }

    public void setString(int slot, String field, String val) {
        int filedOffset = offset(slot) + layout.getFieldOffset(field);
        tx.setString(blk, filedOffset, val, true);
    }

    /********* 删 ***************/

    /**
     * 删除 slot 对应的记录
     * 这里的删除非常简单，slot 对应的记录的标记位设置为 empty
     *
     * @param slot
     */
    public void delete(int slot) {
        this.setFlag(slot, EMPTY);
    }


    /*********** 增 **************/

    /*
     * 在指定的 slot 之后搜索一个 empty slot
     * 如果搜索到，则将 slot 的标记位设置为 USED
     * @param slot
     * @return  slot num that allowed to insert
     */
    public int insertAfter(int slot) {
        int newSlot = this.searchAfter(slot, EMPTY);
        if(newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

    /****** 查 ****************/

    /**
     * 查询下一条有效的记录
     *
     * @param slot
     * @return
     */
    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    private int searchAfter(int slot, int flag) {
        // 从下一个 slot 开始搜索
        slot++;
        // 如果 slot 没有溢出
        while(isValidSlot(slot)) {
            if(tx.getInt(blk, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        // 表示未搜索到
        return -1;
    }

    /**
     * 判断 slot 是否溢出
     *
     * simplexdb 所实现的是 fix length、unspan 的算法，记录不会跨 blk，
     * unspan 的算法可能会导致每个 block 的尾部有一定的空间是浪费的
     * 所以判断一个 slot 是否合法时，如果 next slot 的索引依然在当前 block
     * 中，则该 slot 就能完全保存在当前 block，即合法
     *
     * @param slot
     * @return
     */
    private boolean isValidSlot(int slot) {
        // == 的情况是最好的情况，这表明 blk 没有空间是被浪费的
        return offset(slot + 1) <= tx.blockSize();
    }

    private void setFlag(int slot, int flag) {
        // 写入值，并写入 undolog
        tx.setInt(blk, offset(slot), flag, true);
    }

    /**
     * 计算每个 slot 的索引
     *
     * @param slot
     * @return
     */
    private int offset(int slot) {
        return slot * layout.getSlotSize();
    }
}

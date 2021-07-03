package record;


import file.BlockId;
import query.Constant;
import query.UpdateScan;
import tx.Transaction;

import java.sql.Types;

public class TableScan implements UpdateScan {

    private static final String TABLE_FILE_SUFFIX = ".tbl";

    private Transaction tx;
    private Layout layout;
    private String fileName;

    private RecordPage recordPage;
    private int curSlot;   // 初始化 -1

    public TableScan(Transaction tx, Layout layout, String tableName) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName + TABLE_FILE_SUFFIX;

        if(tx.blockSize() == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }

    }

    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
    public boolean next() {
        int nextSlot = recordPage.nextAfter(curSlot);
        while(nextSlot < 0) {
            if(atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.getBlk().getBlockNum() + 1);
            nextSlot = recordPage.nextAfter(curSlot);
        }
        this.curSlot = nextSlot;
        return true;
    }

    @Override
    public int getInt(String fieldName) {
        return recordPage.getInt(curSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return recordPage.getString(curSlot, fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        int type = layout.getSchema().getType(fieldName);
        if(type == Types.INTEGER) {
            return new Constant(this.getInt(fieldName));
        } else {
            return new Constant(this.getString(fieldName));
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }


    @Override
    public void close() {
        if(recordPage !=  null) {
            tx.unpin(recordPage.getBlk());
        }
    }

    @Override
    public void setVal(String field, Constant val) {
        int type = layout.getSchema().getType(field);
        if(type == Types.INTEGER) {
            this.setInt(field, val.asInt());
        } else {
            this.setString(field, val.asString());
        }
    }

    @Override
    public void setInt(String field, int val) {
        recordPage.setInt(curSlot, field, val);
    }

    @Override
    public void setString(String field, String val) {
        recordPage.setString(curSlot, field, val);
    }

    @Override
    public void insert() {
        int newSlot = recordPage.insertAfter(curSlot);
        while(newSlot < 0) {
            if(atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.getBlk().getBlockNum() + 1);
            }
            newSlot = recordPage.insertAfter(curSlot);
        }
        // 切换到插入的 slot
        this.curSlot = newSlot;
    }

    @Override
    public void delete() {
        recordPage.delete(curSlot);
    }

    @Override
    public RID getRid() {
        return new RID(recordPage.getBlk().getBlockNum(), curSlot);
    }

    @Override
    public void moveToRid(RID rid) {
        this.close();
        BlockId blk = new BlockId(fileName, rid.getBlkNum());
        this.recordPage = new RecordPage(tx, blk, layout);
        this.curSlot = rid.getSlot();
    }


    private void moveToNewBlock() {
        this.close();
        BlockId blk = tx.append(fileName);
        this.recordPage = new RecordPage(tx, blk, layout);
        this.recordPage.format();
        this.curSlot = -1;
    }

    private void moveToBlock(int blkNum) {
        this.close();
        BlockId blk = new BlockId(fileName, blkNum);
        this.recordPage = new RecordPage(tx, blk, layout);
        this.curSlot = -1;
    }

    /**
     * 判断当前是否在最后一个 block
     *
     * @return
     */
    private boolean atLastBlock() {
        return recordPage.getBlk().getBlockNum() == (tx.blockSize() - 1);
    }
}

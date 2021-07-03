package record;

/**
 * record id, 用于定位到某一条记录
 */
public class RID {
    private int blkNum;
    private int slot;

    public RID(int blkNum, int slot) {
        this.blkNum = blkNum;
        this.slot = slot;
    }

    public int getBlkNum() {
        return blkNum;
    }

    public int getSlot() {
        return slot;
    }

    @Override
    public boolean equals(Object o) {
        RID r = (RID) o;
        return blkNum == r.getBlkNum() && slot == r.getSlot();
    }

    @Override
    public String toString() {
        return "record [ " + blkNum + "," + slot + " ]";
    }
}

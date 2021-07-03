package query;

import record.RID;

public interface UpdateScan extends Scan {

    void setVal(String field, Constant val);

    void setInt(String field, int val);

    void setString(String field, String val);

    void insert();

    void delete();

    RID getRid();

    void moveToRid(RID rid);
}

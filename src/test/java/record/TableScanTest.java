package record;

import file.FileManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import server.SimplexDB;
import tx.Transaction;

import java.io.File;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 读写测试
 */
public class TableScanTest {

    @Before
    public void init() {
        File dir = new File("testTableScan");
        File testFie = new File(dir, "simplexdb_students.tbl");
        if(testFie.exists()) {
            testFie.delete();
        }
        File testLofFile = new File(dir, "simplexdb.log");
        if(testLofFile.exists()) {
            testLofFile.delete();
        }
    }

    @After
    public void destory() {
        File dir = new File("testTableScan");
        File testFile = new File(dir, "simplexdb_students.tbl");
        if(testFile.exists()) {
            testFile.delete();
        }
        File testLofFile = new File(dir, "simplexdb.log");
        if(testLofFile.exists()) {
            testLofFile.delete();
        }
        dir.delete();
    }

    @Test
    public void testReadAndWriteTable() {
        SimplexDB simplexDB = new SimplexDB("testTableScan", SimplexDB.DEFAULT_BLK_SIZE);
        Transaction tx = simplexDB.newTx();

        Schema schema = new Schema();
        schema.addStringField("student_name", 12);
        schema.addIntField("age");
        schema.addIntField("grade");
        schema.addIntField("class");
        schema.addStringField("student_no", 32);
        Layout layout = new Layout(schema);

        TableScan tableScan = new TableScan(tx, layout, "simplexdb_students");

//        Assert.assertTrue(tableScan.atLastBlock());
        Assert.assertEquals(0, tableScan.getRid().getBlkNum());
        Assert.assertEquals(-1, tableScan.getRid().getSlot());

        // 先写数据
        // 随机生成要写入多少行数据
        Integer randomRow = ThreadLocalRandom.current().nextInt(100);
        Map<Integer, String> recordMap = new HashMap<>();
        for(int i = 0; i < randomRow; i++) {
            // 写入一行数据
            String record = writeRow(layout, tableScan);
            System.out.println("reocrd [ " + i + " ]: " + record);
            recordMap.put(i, record);
        }
        // 切换到第一行记录
        RID rid = new RID(0, -1);
        tableScan.moveToRid(rid);
        // 读取数据并校验是否与写入的一致
        for(int i = 0; i < randomRow; i++) {
            readAndCheck(recordMap.get(i), layout, tableScan);
        }

        // 随机删除其中一条数据
        int removeRow = ThreadLocalRandom.current().nextInt(randomRow);
        // 计算每个块有多少条记录
        int rowsPerBlk = tx.blockSize() / layout.getSlotSize();
        int blkNum = removeRow / rowsPerBlk;
        int slot = removeRow - blkNum * rowsPerBlk - 1;  // slot 从 0 开始算的，比如删除第一行，那么 slot 就是 0;
        RID removeRid = new RID(blkNum, slot);
        tableScan.moveToRid(removeRid);
        tableScan.delete();
        // 统计数量
        tableScan.moveToRid(rid);
        Assert.assertEquals((long)randomRow - 1, (long)countRwow(tableScan));
    }

    private int countRwow(TableScan tableScan) {
        int count = 0;
        while(tableScan.next()) {
            count++;
        }
        return count;
    }

    private String writeRow(Layout layout, TableScan tableScan) {
        tableScan.insert();
        Schema schema = layout.getSchema();
        String record = "";
        for(String field : schema.getFieldNameList()) {
            int type = schema.getType(field);
            String val = null;
            if(type == Types.INTEGER) {
                Integer ival = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                tableScan.setInt(field, ival);
                val = ival.toString();
            } else if(type == Types.VARCHAR) {
                String uuid = UUID.randomUUID().toString();
                String sval = uuid.substring(0, schema.getLength(field));
                tableScan.setString(field, sval);
                val = sval;
            }
            record = record + val + ",";
        }

        return record;
    }

    private void readAndCheck(String record, Layout layout, TableScan tableScan) {
        // 先读取一行数据
        if(!tableScan.next()) {
            return;
        }
        String[] row = record.split(",");
        Schema schema = layout.getSchema();
        int i = 0;
        for(String field : schema.getFieldNameList()) {
            String fieldVal = null;
            int type = schema.getType(field);
            if(type == Types.INTEGER) {
                Integer ival = tableScan.getInt(field);
                fieldVal = ival.toString();
            } else if(type == Types.VARCHAR) {
                fieldVal = tableScan.getString(field);
            }
            Assert.assertEquals(row[i], fieldVal);
            i++;
        }

    }
}

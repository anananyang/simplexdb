package record;

import buffer.BufferManager;
import file.BlockId;
import file.FileManager;
import log.LogManager;
import org.junit.*;
import org.junit.runners.MethodSorters;
import server.SimplexDB;
import tx.Transaction;

import java.io.File;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 测试对 block 的读写是否正确
 */

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class RecordPageTest {
    private static Layout layout;
    private static RecordPage recordPage;
    private static FileManager fileManager;
    private static LogManager logManager;
    private static BufferManager bufferManager;


    private static File datebase;
    private static File table;
    private static String TEST_DIR_PATH = "reocrdTest";
    private static String TEST_TABLE_PATH = "simple_db_students.tbl";
    private static String TEST_LOG_PATH = "recordTest.log";

    @BeforeClass
    public static void init() {
        datebase = new File(TEST_DIR_PATH);
        File table = new File(datebase, TEST_TABLE_PATH);
        if (table.exists()) {
            table.delete();
        }

        Schema schema = new Schema();
        schema.addStringField("student_name", 12);
        schema.addIntField("age");
        schema.addIntField("grade");
        schema.addIntField("class");
        schema.addStringField("student_no", 32);
        layout = new Layout(schema);


        fileManager = new FileManager(datebase, SimplexDB.DEFAULT_BLK_SIZE);
        logManager = new LogManager(fileManager, TEST_LOG_PATH);
        bufferManager = new BufferManager(fileManager, logManager, SimplexDB.DEFAULT_BUFFER_SIZE);

        Transaction tx = new Transaction(fileManager, logManager, bufferManager);
        BlockId blockId = new BlockId(TEST_TABLE_PATH, 0);
        recordPage = new RecordPage(tx, blockId, layout);
    }

    @AfterClass
    public static void destory() {
        File table = new File(datebase, TEST_TABLE_PATH);
        if (table.exists()) {
            table.delete();
        }
        datebase.delete();
    }

    @Test
    public void testA1FormatAndInsertAfterAndNextAfter() {
        // 先做格式化
        recordPage.format();
        Integer slotSize = layout.getSlotSize();
        // 计算有多少个 slot
        int slotNum = fileManager.getBlockSize() / slotSize;
        // 从第一个开始
        int curSlot = -1;
        for (; curSlot < slotNum; curSlot++) {
            int insertSlot = recordPage.insertAfter(curSlot);
            if (curSlot == slotNum - 1) {
                Assert.assertEquals(insertSlot, -1);
            } else {
                Assert.assertEquals(insertSlot, (curSlot + 1));
            }
        }
        curSlot = -1;
        for (; curSlot < slotNum; curSlot++) {
            int nextSlot = recordPage.nextAfter(curSlot);
            if (curSlot == slotNum - 1) {
                Assert.assertEquals(nextSlot, -1);
            } else {
                Assert.assertEquals(nextSlot, (curSlot + 1));
            }
        }

    }

    @Test
    public void testB2Deleted() {
        // 先做格式化
        recordPage.format();
        Integer slotSize = layout.getSlotSize();
        // 计算有多少个 slot
        int slotNum = fileManager.getBlockSize() / slotSize;
        // 从第一个开始
        int curSlot = -1;
        for (; curSlot < slotNum; curSlot++) {
            int insertSlot = recordPage.insertAfter(curSlot);
            if (insertSlot == -1) {
                break;
            }
        }
        // 删除
        for (curSlot = 0; curSlot < slotNum; curSlot++) {
            // 将其删除
            recordPage.delete(curSlot);
        }

        // 删除完之后应该是没数据的
        Assert.assertEquals(-1, recordPage.nextAfter(-1));
    }

    /**
     * 读写测试
     */
    @Test
    public void testC3ReadAndWrite() {
        // 先做格式化
        recordPage.format();
        Integer slotSize = layout.getSlotSize();
        // 计算有多少个 slot
        int slotNum = fileManager.getBlockSize() / slotSize;
        // 从第一个开始
        int curSlot = -1;
        Map<Integer, String> recordMap = new TreeMap<>();
        for (; curSlot < slotNum; curSlot++) {
            int insertSlot = recordPage.insertAfter(curSlot);
            if(insertSlot == -1) {
                break;
            }
            Schema schema = layout.getSchema();
            String record = "";
            for(String field : schema.getFieldNameList()) {
                if(schema.getType(field) == Types.INTEGER) {
                    Integer ival = ThreadLocalRandom.current().nextInt(100);
                    recordPage.setInt(insertSlot, field, ival);
                    record = record + ival + ",";
                } else if (schema.getType(field) == Types.VARCHAR) {
                    String uuid = UUID.randomUUID().toString();
                    String sval = uuid.substring(0, schema.getLength(field));
                    recordPage.setString(insertSlot, field, sval);
                    record = record + sval + ",";
                }
            }
            recordMap.put(insertSlot, record);
        }


        // 开始校验
        curSlot = -1;
        for (; curSlot < slotNum; curSlot++) {
            int nextSlot = recordPage.nextAfter(curSlot);
            if(nextSlot == -1) {
                break;
            }
            Schema schema = layout.getSchema();
            String recordStr = recordMap.remove(nextSlot);
            int i = 0;
            String[] record = recordStr.split(",");
            for(String field : schema.getFieldNameList()) {
                String val = null;
                if (schema.getType(field) == Types.INTEGER) {
                    Integer ival = recordPage.getInt(nextSlot, field);
                    if (ival != null) {
                        val = ival.toString();
                    }
                } else if (schema.getType(field) == Types.VARCHAR) {
                    val = recordPage.getString(nextSlot, field);
                }

                Assert.assertEquals(val, record[i]);
                i++;
            }
        }

        Assert.assertEquals(0, recordMap.size());
    }



}

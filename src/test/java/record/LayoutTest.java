package record;

import file.Page;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class LayoutTest {

    private static Schema schema;
    private static Integer slotSize;
    private static Map<String, Integer> fieldOffset;
    private static Layout layout;

    @BeforeClass
    public static void init() {
        schema = new Schema();
        schema.addStringField("student_name", 12);
        schema.addIntField("age");
        schema.addIntField("grade");
        schema.addIntField("class");
        schema.addStringField("student_no", 32);

        fieldOffset = new HashMap<>();
        int offset = Integer.BYTES;  // flag
        for (String field : schema.getFieldNameList()) {
            fieldOffset.put(field, offset);
            int type = schema.getType(field);
            if(type == Types.INTEGER) {
                offset += Integer.BYTES;
            } else if(type == Types.VARCHAR){
                offset += Page.maxLength(schema.getLength(field));
            }
        }
        slotSize = offset;
        layout = new Layout(schema);
    }

    @Test
    public void testA1GetSlotSize() {
        Assert.assertEquals((long) slotSize, layout.getSlotSize());
    }

    @Test
    public void TestB2getFieldOffset() {
        String[] fields = {"student_name", "age", "grade", "class", "student_no"};
        for(String field : fields) {
            Assert.assertEquals((long)layout.getFieldOffset(field), (long)fieldOffset.get(field));
        }
    }


}

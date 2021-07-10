package record;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Types;
import java.util.concurrent.ThreadLocalRandom;

/**
 * schema 测试增加字段的几个方法是否正确
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class SchemaTest {

    @Test
    public void testaddField() {
        String testIntField = "testIntField";
        String testStringField = "testStringField";
        Integer strLen = ThreadLocalRandom.current().nextInt(100);

        Schema schema = new Schema();
        Assert.assertTrue(schema.getFieldNameList().size() == 0);

        schema.addField("testIntField", Types.INTEGER, Integer.BYTES);
        // 校验字段是否存在，校验字段类型是否正确，校验字段的长度是否正确
        Assert.assertTrue(schema.hasField(testIntField));
        Assert.assertTrue(schema.getType(testIntField) == Types.INTEGER);
        Assert.assertTrue(schema.getLength(testIntField) == Integer.BYTES);
        Assert.assertTrue(schema.getFieldNameList().size() == 1);
        Assert.assertTrue(schema.getFieldNameList().size() == 1);

        schema.addField("testStringField", Types.VARCHAR, strLen);
        Assert.assertTrue(schema.hasField(testStringField));
        Assert.assertTrue(schema.getType(testStringField) == Types.VARCHAR);
        Assert.assertTrue(schema.getLength(testStringField) == strLen);
        Assert.assertTrue(schema.getFieldNameList().size() == 2);

    }

    @Test
    public void testaddFieldFromOtherSchemaField() {
        String testIntField = "testIntField";
        String testStringField = "testStringField";
        Integer strLen = ThreadLocalRandom.current().nextInt(100);

        Schema sourceSchema = new Schema();
        sourceSchema.addField("testIntField", Types.INTEGER, Integer.BYTES);
        sourceSchema.addField("testStringField", Types.VARCHAR, strLen);

        Schema targetSchema = new Schema();
        targetSchema.add(testIntField, sourceSchema);
        Assert.assertTrue(targetSchema.hasField(testIntField));
        Assert.assertTrue(targetSchema.getType(testIntField) == Types.INTEGER);
        Assert.assertTrue(targetSchema.getLength(testIntField) == Integer.BYTES);
        Assert.assertTrue(targetSchema.getFieldNameList().size() == 1);


        targetSchema.add(testStringField, sourceSchema);
        Assert.assertTrue(targetSchema.hasField(testStringField));
        Assert.assertTrue(targetSchema.getType(testStringField) == Types.VARCHAR);
        Assert.assertTrue(targetSchema.getLength(testStringField) == strLen);
        Assert.assertTrue(targetSchema.getFieldNameList().size() == 2);
    }

    @Test
    public void testaddFieldFromOtherSchema() {
        String testIntField = "testIntField";
        String testStringField = "testStringField";
        Integer strLen = ThreadLocalRandom.current().nextInt(100);

        Schema sourceSchema = new Schema();
        sourceSchema.addField("testIntField", Types.INTEGER, Integer.BYTES);
        sourceSchema.addField("testStringField", Types.VARCHAR, strLen);

        Schema targetSchema = new Schema();
        targetSchema.addAll(sourceSchema);
        Assert.assertTrue(targetSchema.getFieldNameList().size() == 2);

        Assert.assertTrue(targetSchema.hasField(testIntField));
        Assert.assertTrue(targetSchema.getType(testIntField) == Types.INTEGER);
        Assert.assertTrue(targetSchema.getLength(testIntField) == Integer.BYTES);

        targetSchema.add(testStringField, sourceSchema);
        Assert.assertTrue(targetSchema.hasField(testStringField));
        Assert.assertTrue(targetSchema.getType(testStringField) == Types.VARCHAR);
        Assert.assertTrue(targetSchema.getLength(testStringField) == strLen);
    }

    @Test
    public void testAddIntField() {
        String testIntField = "testIntField";
        Schema schema = new Schema();
        schema.addIntField("testIntField");

        Assert.assertTrue(schema.hasField(testIntField));
        Assert.assertTrue(schema.getType(testIntField) == Types.INTEGER);
        Assert.assertTrue(schema.getLength(testIntField) == Integer.BYTES);
    }

    @Test
    public void testAddStringField() {
        String testStringField = "testStringField";
        Integer strLen = ThreadLocalRandom.current().nextInt(100);

        Schema schema = new Schema();
        schema.addStringField("testStringField", strLen);

        Assert.assertTrue(schema.hasField(testStringField));
        Assert.assertTrue(schema.getType(testStringField) == Types.VARCHAR);
        Assert.assertTrue(schema.getLength(testStringField) == strLen);
    }
}

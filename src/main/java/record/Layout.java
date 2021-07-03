package record;

import file.Page;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Layout 包含两个信息
 * 1. record 的长度
 * 2. record 每个字段的位置（ps：这里我们实现是最简单的方案，所有的字段的长度都是定长的）
 *
 */
public class Layout {
    // record 的长度
    private int slotSize;
    // record 每个字段的位置
    private Map<String, Integer> fieldOffsetMap;
    // 根据 schema 中的字段信息计算每个字段的索引
    private Schema schema;

    /**
     * 通过 schema 计算 record 的长度，以及 record 中每个字段的索引位置
     * NOTE: 每个 record 的开头是一个标记字段（simplexdb 中使用一个 int 来表示）
     * 如果标记字段为1，表示该记录使用（use），标记字段为0，表示该记录未使用/删除(empyt)
     *
     * |----------------|---------------------------------|
     * |   record flag  |           record content        |
     * |----------------|---------------------------------|
     *
     * 这个构造器在 table 被创建时使用，计算每个字段在 record 中的索引
     *
     * @param schema
     */
    public Layout(Schema schema) {
        this.schema = schema;
        this.fieldOffsetMap = new HashMap<>();
        // 每条记录的字节长度（当前的我们使用的是定长字段去实现）
        int pos = 0;
        pos = pos + Integer.BYTES;   // 标记位的长度
        List<String> filedList = schema.getFieldNameList();
        for(String field : filedList) {
            fieldOffsetMap.put(field, pos);
            pos = pos + this.lengthInBytes(field);
        }
        this.slotSize = pos;
    }

    /**
     * 通过从 metadata 中取出 table 的信息来创建 Layout
     *
     * @param schema
     * @param fieldOffsetMap
     * @param slotSize
     */
    public Layout(Schema schema, Map<String, Integer> fieldOffsetMap, int slotSize) {
        this.schema = schema;
        this.fieldOffsetMap = fieldOffsetMap;
        this.slotSize = slotSize;
    }

    /**
     * 根据字段的类型计算字段的字节长度
     * NOTE: 当前我们只实现两种类型
     *
     * @param filedName
     * @return
     */
    private int lengthInBytes(String filedName) {
        int type = schema.getType(filedName);
        if(type == Types.INTEGER) {
            return Integer.BYTES;
        } else {
            // 根据字符串的长度计算
            return Page.maxLength(schema.getLength(filedName));
        }
    }

    /**
     * 获取每条记录的长度
     *
     * @return
     */
    public int getSlotSize() {
        return slotSize;
    }

    /**
     * 根据字段名称获取每个字段在记录中的索引
     *
     * @param fieldName
     * @return
     */
    public int getFieldOffset(String fieldName) {
        return fieldOffsetMap.get(fieldName);
    }

    public Schema getSchema() {
        return schema;
    }
}

package record;

import util.ListUtil;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 记录了 table 有那些字段，以及每个字段的信息
 */
public class Schema {

    private List<String> fieldNameList = new ArrayList<>();
    private Map<String, FieldInfo> fieldInfoMap = new HashMap<>();

    public void addField(String fieldName, int type, int length) {
        fieldNameList.add(fieldName);
        fieldInfoMap.put(fieldName, new FieldInfo(type, length));
    }

    public void addIntField(String fieldName) {
        // 实际上不会使用这个的长度 Integer.BYTES
        addField(fieldName, Types.INTEGER, Integer.BYTES);
    }

    /**
     *
     * @param fieldName
     * @param length  字符串中字符的数量
     */
    public void addStringField(String fieldName, int length) {
        addField(fieldName, Types.VARCHAR, length);
    }

    /**
     * 将其他 schema 中的字段添加到当前 schema 中
     */
    public void add(String fieldName, Schema schema) {
        addField(fieldName, schema.getType(fieldName), schema.getLength(fieldName));
    }

    /**
     * 将其他 schema 中的字段添加到当前 schema 中
     */
    public void addAll(Schema schema) {
        List<String> fieldNameList = schema.getFieldNameList();
        if(ListUtil.isBlank(fieldNameList)) {
            return;
        }
        for(String fieldName : fieldNameList) {
            int type = schema.getType(fieldName);
            int length = schema.getLength(fieldName);
            FieldInfo fieldInfo = new FieldInfo(type, length);
            fieldInfoMap.put(fieldName, fieldInfo);
            this.fieldNameList.add(fieldName);
        }
    }

    public Boolean hasField(String fieldName) {
        return fieldNameList.contains(fieldName);
    }

    /**
     * 获取当前 schema 中的所有字段名称
     *
     * @return
     */
    public List<String> getFieldNameList() {
        return fieldNameList;
    }

    /**
     * 获取字段类型
     *
     * @param fieldName
     * @return
     */
    public int getType(String fieldName) {
        return fieldInfoMap.get(fieldName).getType();
    }

    /**
     * 获取字段长度
     *
     * @param fieldName
     * @return
     */
    public int getLength(String fieldName) {
        return fieldInfoMap.get(fieldName).getLength();
    }

}

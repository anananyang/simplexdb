package record;

/**
 * table 中每个字段的信息
 */
public class FieldInfo {
    // 字段类型, 这里的 type 使用 java.sql.Types 中的类型
    private Integer type;
    // 字段长度
    private Integer length;

    FieldInfo(Integer type, Integer length) {
        this.type = type;
        this.length = length;
    }

    public Integer getType() {
        return type;
    }

    public Integer getLength() {
        return length;
    }
}

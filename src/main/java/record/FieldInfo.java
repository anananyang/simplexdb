package record;

/**
 * table 中每个字段的信息
 */
public class FieldInfo {
    // 字段类型, 这里的 type 使用 java.sql.Types 中的类型
    private Integer type;
    // length 主要用来记录 vchar 类型的长度，这里表示字符串的长度，而不是字节的长度
    // 如果是 int 类型，则 length 为 0
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

package record;

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
    private Schema schema;




}

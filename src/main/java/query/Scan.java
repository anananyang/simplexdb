package query;

public interface Scan {
    /**
     * 在扫描之前将扫描器的指针定位在第一条记录之前，
     * 接着如果调用 next 会返回第一条记录
     */
    void beforeFirst();

    /**
     * 将扫描器的指针定位到下一条记录
     *
     * @return
     */
    boolean next();

    int getInt(String fieldName);

    String getString(String fieldName);

    Constant getVal(String fieldName);

    boolean hasField(String fieldName);

    /**
     * 关闭扫描器以及相应的子扫描器（如果存在）
     */
    void close();
}

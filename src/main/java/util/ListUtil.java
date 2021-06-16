package util;

import java.util.List;

public abstract class ListUtil {

    public static boolean isBlank(List<?> list) {
        return list == null || list.size() == 0;
    }
}

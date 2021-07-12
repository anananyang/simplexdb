package util;

import java.io.File;

public abstract class FileUtil {

    public static void removeDir(File dir) {
        if(!dir.isDirectory()) {
            return;
        }
        for(File file : dir.listFiles()) {
            if(file.isDirectory()) {
                removeDir(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }
}

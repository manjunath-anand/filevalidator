package org.anz.codechallenge.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * File Util class for common file operations
 */
public class FileUtil {

    /**
     * Read entire filecontents
     * @param filePath - path where file is present
     * @return
     */
    public static String readAllBytes(String filePath) {
        String content = "";
        try {
            content = new String ( Files.readAllBytes( Paths.get(filePath) ) );
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return content;
    }
}

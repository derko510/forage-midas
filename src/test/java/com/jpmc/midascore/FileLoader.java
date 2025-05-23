package com.jpmc.midascore;

import org.springframework.stereotype.Component;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.InputStream;

@Component
public class FileLoader {
    public String[] loadStrings(String path) {
        try {
            InputStream inputStream = this.getClass().getResourceAsStream(path);
            String fileText = IOUtils.toString(inputStream, "UTF-8");
            // Split on newlines and trim each line
            return fileText.lines()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .toArray(String[]::new);
        } catch (Exception e) {
            return null;
        }
    }
}

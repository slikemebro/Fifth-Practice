package com.ua.glebkorobov;

import com.ua.glebkorobov.exception.FileFindException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

public class GetProperty {
    private final String fileNameProperty;
    private static final Logger logger = LogManager.getLogger(GetProperty.class);
    private final Properties properties = new Properties();

    public GetProperty(String fileNameProperty) {
        logger.info("Loaded properties file");
        this.fileNameProperty = fileNameProperty;
        loadFile();
    }

    private void loadFile() {
        try {
            InputStream is = GetProperty.class.getResourceAsStream("/" + fileNameProperty);
            properties.load(is);
            logger.info("Getting properties");
        } catch (Exception e) {
            logger.warn("Properties hasn't found");
            throw new FileFindException("File hasn't found");
        }
    }

    public String getValueFromProperty(String key) {
        logger.info("Got {} from properties", key);
        return properties.getProperty(key.toLowerCase(Locale.ROOT));
    }
}

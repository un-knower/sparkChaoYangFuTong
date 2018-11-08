package com.ald.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by luanma on 2017/5/14.
 */
public class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        try {
            properties = getConfig("/config.properties");
            if (properties == null) throw new Exception("Can't load config !");
        } catch (Exception e) {
            logger.error("", e);
        }

    }

    public static String get(String name) {
        return properties.getProperty(name);
    }

    public static String get(String name, String d) {
        return properties.getProperty(name, d);
    }

    /**
     * 返回配置文件
     *
     * @param filePath 文件路径
     * @return
     * @throws IOException
     */
    public static Properties getConfig(final String filePath) {
        InputStream inputStream = null;
        Properties prop = new Properties();
        try {
            inputStream = ConfigUtils.class.getResourceAsStream(filePath);
            logger.debug("inputStreaminputStream:" + inputStream + "-" + ConfigUtils.class.getResource(filePath));
            prop.load(inputStream);
        } catch (IOException e) {
            logger.error("", e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return prop;
    }

    public static void main(String[] args) throws IOException {
        logger.info("default  -> " + ConfigUtils.get("redis.write.pool"));

        Properties prop = ConfigUtils.getConfig("/config.properties");
        logger.info("from file -> " + prop.getProperty("redis.write.pool"));
    }
}

package com.ald.finder.qqwry;

import com.ald.Contants;
import com.ald.GeoFinder;
import com.ald.IpLocation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;

/**
 * Created by luanma on 2017/5/14.
 */
public class QQWRYFinder implements GeoFinder {
    private static final Logger logger = LoggerFactory.getLogger(QQWRYFinder.class);

    private IpSeekerSimpleCache ipSeeker;

    public QQWRYFinder(String filepath) throws IOException {
        logger.info("read QQWR db file: " + filepath);
        File file = new File(filepath);
        if (file.exists()) {
            byte[] bytes = FileUtils.readFileToByteArray(file);
            ipSeeker = new IpSeekerSimpleCache(bytes);
        } else {
            throw new FileNotFoundException();
        }
    }

    public QQWRYFinder(InputStream inputStream) throws IOException {
        byte[] bytes = IOUtils.toByteArray(inputStream);
        ipSeeker = new IpSeekerSimpleCache(bytes);
    }

    public static void main(String[] args) {
        try {
            String[] ips = Contants.TEST_IPS;
            GeoFinder finder = new QQWRYFinder("/workspace/wcsa/datacenter/pirate/src/main/data/qqwry.dat");
            for (String ip : ips) {
                IpLocation location = finder.find(ip);
                logger.info("{} -> {}", ip, location);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public IpLocation find(String ip) {
        try {
            return ipSeeker.getLocation(Inet4Address.getByName(ip));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
}

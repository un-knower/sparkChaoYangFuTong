package com.ald.finder.geoip;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.ald.Contants;
import com.ald.GeoFinder;
import com.ald.IpLocation;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Collections;

/**
 * Created by luanma on 2017/5/14.
 */
public class GeoIPFinder implements GeoFinder {
    private static final Logger logger = LoggerFactory.getLogger(GeoIPFinder.class);

    private static DatabaseReader dbReader = null;

    public GeoIPFinder(String filePath) throws IOException {
        logger.info("read geo db file: " + filePath);
        File file = new File(filePath);
        dbReader = new DatabaseReader.Builder(file).locales(Collections.singletonList("zh-CN")).withCache(new CHMCache()).build();
    }
    public GeoIPFinder(InputStream inputStream) throws IOException {
        dbReader = new DatabaseReader.Builder(inputStream).locales(Collections.singletonList("zh-CN")).withCache(new CHMCache()).build();
    }

    public static void main(String[] args) {
        try {
            String[] ips = Contants.TEST_IPS;
            GeoFinder finder = new GeoIPFinder("/workspace/wcsa/datacenter/pirate/src/main/data/GeoLite2-City.mmdb");
            for (String ip : ips) {
                IpLocation location = finder.find(ip);
                logger.info("{} -> {}", ip, location);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public IpLocation find(String ip) {
        if (dbReader == null) {
            logger.error("GeoDB can't initial !");
            return null;
        }

        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            CityResponse cityResponse = dbReader.city(ipAddress);

            IpLocation ipLocation = new IpLocation();
            if (StringUtils.isNotEmpty(cityResponse.getCity().getName())) {
                ipLocation.setCity(cityResponse.getCity().getName());
            }

            if (StringUtils.isNotEmpty(cityResponse.getMostSpecificSubdivision().getName())) {
                ipLocation.setProvince(cityResponse.getMostSpecificSubdivision().getName());
            }

            if (StringUtils.isNotEmpty(cityResponse.getCountry().getName())) {
                ipLocation.setCountry(cityResponse.getCountry().getName());
                ipLocation.setIsoCode(cityResponse.getCountry().getIsoCode());
            }

            return ipLocation;
        } catch (Exception e) {
            logger.warn("found ip failed :" + e.getMessage());
        }
        return null;
    }

    @Override
    protected void finalize() throws Throwable {
        if (dbReader != null) {
            dbReader.close();
        }
    }
}

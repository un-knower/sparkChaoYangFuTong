package com.ald;

import com.ald.cache.RedisCache;
import com.ald.finder.geoip.GeoIPFinder;
import com.ald.finder.local.LocalFinder;
import com.ald.finder.qqwry.QQWRYFinder;
import com.ald.util.IPUtils;
import com.ald.util.Segment;
import com.ald.util.Segment2CacheInitor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by luanma on 2017/5/14.
 */
public class FinderManager implements Contants {
    private static final Logger logger = LoggerFactory.getLogger(FinderManager.class);

    private final List<GeoFinder> homeFinders = new ArrayList<GeoFinder>();
    private final List<GeoFinder> overseaFinders = new ArrayList<GeoFinder>();
    private final Map<String, IpLocation> cache;

    private RedisCache redisCache;

    public FinderManager() {
        Map<String, IpLocation> map = new WeakHashMap();
        cache = Collections.synchronizedMap(map);
    }

    public static FinderManager builder() {
        try {
            FinderManager manager = new FinderManager();
            manager.homeFinders.add(new LocalFinder());
            manager.homeFinders.add(new QQWRYFinder(LocalFinder.class.getClassLoader().getResourceAsStream("qqwry.dat")));
            manager.overseaFinders.add(new GeoIPFinder(LocalFinder.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb")));
            return manager;
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean ipInChina(String ip) {
        long ipLong = IPUtils.ipToLong(ip);
        for (Segment segment : Segment2CacheInitor.getInstance().list) {
            if (ipLong >= segment.begin && ipLong <= segment.end) {
                return true;
            }
        }
        return false;
    }

    public static void main(String... args) {
        try {
            String[] ips = Contants.TEST_IPS;
            FinderManager manager = FinderManager.builder();
//            manager.homeFinders.add(new LocalFinder());
//            manager.homeFinders.add(new QQWRYFinder("/Users/lemanli/work/project/newcma/wcsa/wcsa_datacenter/pirate/src/main/data/qqwry.dat"));
//            manager.overseaFinders.add(new GeoIPFinder("/Users/lemanli/work/project/newcma/wcsa/wcsa_datacenter/pirate/src/main/data/GeoLite2-City.mmdb"));

//            manager.homeFinders.add(new QQWRYFinder(LocalFinder.class.getClassLoader().getResourceAsStream("qqwry.dat")));
//            manager.overseaFinders.add(new GeoIPFinder(LocalFinder.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb")));

            for (String ip : ips) {
                logger.info("{} -> {}", ip, ipInChina(ip) ? "home" : "oversea");
                logger.info("{} -> {}", ip, manager.getLocation(ip));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public RedisCache getRedisCache() {
        return redisCache;
    }

    public void setRedisCache(RedisCache redisCache) {
        this.redisCache = redisCache;
    }

    public List<GeoFinder> getHomeFinders() {
        return homeFinders;
    }

    public List<GeoFinder> getOverseaFinders() {
        return overseaFinders;
    }

    public IpLocation getLocation(String ip) {
        if (StringUtils.isBlank(ip)) {
            return null;
        }
//        String ipKey = ipKey(ip);
//        IpLocation ipLocation = getCache(ipKey, ip);
        IpLocation ipLocation = null;
        if (ipLocation == null) {
            boolean isInChina = ipInChina(ip);
            ipLocation = findLocation(isInChina, ip);
            if (ipLocation == null) {
                ipLocation = findLocation(!isInChina, ip);
            }
//            if (ipLocation != null) {
//                putCache(ipKey, ip, ipLocation);
//            }
        }

        return ipLocation;
    }

    private IpLocation findLocation(boolean isInChina, String ip) {
        IpLocation ipLocation = null;
        if (isInChina) {
            for (GeoFinder finder : homeFinders) {
                ipLocation = finder.find(ip);
                if (ipLocation != null && StringUtils.isNotEmpty(ipLocation.getProvince()) && StringUtils.isNotEmpty(ipLocation.getCity())) {
                    if (ipLocation.getCountry() != null && "中国".equals(ipLocation.getCountry())) {
                        ipLocation.setIsoCode("CN");
                    } else {
                        ipLocation.setCountry("中国");
                        ipLocation.setIsoCode("CN");
                    }

                    break;
                }
            }
        } else {
            for (GeoFinder finder : overseaFinders) {
                ipLocation = finder.find(ip);
                if (ipLocation != null) {
                    break;
                }
            }
        }

        return ipLocation;
    }

    private String ipKey(String ip) {
        return IPUtils.ipFirst(ip) + IP_KEY_PERFIX;
    }

    private IpLocation getCache(String ipKey, String ip) {
        if (getRedisCache() != null) {
            try {
                return getRedisCache().hget2Object(ipKey(ip), ip, IpLocation.class);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return cache.get(ipKey);
            }
        } else {
            return cache.get(ipKey);
        }
    }

    private void putCache(String ipKey, String ip, IpLocation location) {
        if (getRedisCache() != null) {
            try {
                getRedisCache().hset2Object(ipKey, ip, location);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                cache.put(ipKey, location);
            }
        } else {
            cache.put(ipKey, location);
        }
    }
}

package com.ald.finder.local;

import com.ald.Contants;
import com.ald.GeoFinder;
import com.ald.IpLocation;
import com.ald.util.IPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class LocalFinder implements GeoFinder {
    private static final Logger logger = LoggerFactory.getLogger(LocalFinder.class);

    private static final String FILE_CITY_INFO = "cityinfo.csv";
    private static final String FILE_IP_INFO = "ipinfo.csv";

    private final Map<String, String> cityInfos;
    private final List<String> ipInfos;

    public LocalFinder() {
        Map<String, String> map = new HashMap<>();
        try (InputStream inputStream = LocalFinder.class.getClassLoader().getResourceAsStream(FILE_CITY_INFO)) {
            LineIterator lineIterator = IOUtils.lineIterator(inputStream, "UTF-8");
            while (lineIterator != null && lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                String[] fields = StringUtils.split(line, '=');
                if (fields != null && fields.length == 2) {
                    map.put(fields[0], fields[1]);
                }
            }

            if (lineIterator != null) {
                lineIterator.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        cityInfos = Collections.synchronizedMap(map);

        List<String> list = new ArrayList<>();
        try (InputStream inputStream = LocalFinder.class.getClassLoader().getResourceAsStream(FILE_IP_INFO)) {
            LineIterator lineIterator = IOUtils.lineIterator(inputStream, "UTF-8");
            while (lineIterator != null && lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                if (StringUtils.isNotBlank(line)) {
                    list.add(line);
                }
            }

            if (lineIterator != null) {
                lineIterator.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        ipInfos = Collections.synchronizedList(list);

        logger.info("LocalFinder inited. Loaded {} cities, {} ips.", cityInfos.size(), ipInfos.size());
    }

    public static void main(String... args) {
        String[] ips = Contants.TEST_IPS;
        GeoFinder finder = new LocalFinder();

        for (String ip : ips) {
            IpLocation location = finder.find(ip);
            logger.info("{} -> {}", ip, location);
        }
    }

    @Override
    public IpLocation find(String ip) {
        String cityId = findCityId(ip);
//        logger.info("{} -> cityId[{}]", ip, cityId);
        if (StringUtils.isNotEmpty(cityId)) {
            IpLocation location = new IpLocation();
            location.setLocationCode(cityId);

            location.setCountry("中国");
            location.setIsoCode("CN");

            String city = cityInfos.get(cityId);
            String[] fields = StringUtils.split(city, ',');
            if (fields != null && fields.length >= 3) {
                location.setProvince(fields[0]);
                location.setCity(fields[1]);
            }
            return location;
        }
        return null;
    }

    @Override
    protected void finalize() throws Throwable {
    }

    private String findCityId(String ip) {
        String hex = IPUtils.toHex(ip);
        int pre = 0;
        int bck = ipInfos.size();
        int mid = (pre + bck) / 2;

        while (pre <= bck) {
            String msg = ipInfos.get(mid - 1);
            String[] fields = decodeMessage(msg);
            int preCmp = compareHex(fields[0], hex);
            int bckCmp = compareHex(fields[1], hex);

            if (preCmp <= 0 && 0 <= bckCmp) {
                break;
            } else if (0 < preCmp) {
                bck = mid - 1;
            } else if (bckCmp < 0) {
                pre = mid + 1;
            }

            mid = (int) Math.floor((pre + bck) / 2.0);
        }

//        logger.info("pre={}, mid={}, bck={}", new Object[] {pre, mid, bck});
        if (pre > bck) {
            {
                String cityId = getCityId(hex, bck - 1);
                if (cityId != null) {
                    return cityId;
                }
            }

            {
                String cityId = getCityId(hex, pre - 1);
                if (cityId != null) {
                    return cityId;
                }
            }

            return null;
        }

        String msg = ipInfos.get(mid - 1);
        return StringUtils.substring(msg, -9);
    }

    private String getCityId(String hex, int index) {
        String msg = ipInfos.get(index);
        String[] fields = decodeMessage(msg);
        int preCmp = compareHex(fields[0], hex);
        int bckCmp = compareHex(fields[1], hex);

        if (preCmp <= 0 && 0 <= bckCmp) {
            return StringUtils.substring(msg, -9);
        } else {
            return null;
        }
    }

    public static final long fromUnsignedHex(final String valueInUnsignedHex) {
        long value = 0;

        final int hexLength = valueInUnsignedHex.length();
        if (hexLength == 0) throw new NumberFormatException("For input string: \"\"");
        for (int i = Math.max(0, hexLength - 16); i < hexLength; i++) {
            final char ch = valueInUnsignedHex.charAt(i);

            if      (ch >= '0' && ch <= '9') value = (value << 4) | (ch - '0'         );
            else if (ch >= 'A' && ch <= 'F') value = (value << 4) | (ch - ('A' - 0xaL));
            else if (ch >= 'a' && ch <= 'f') value = (value << 4) | (ch - ('a' - 0xaL));
            else                             throw new NumberFormatException("For input string: \"" + valueInUnsignedHex + "\"");
        }

        return value;
    }
    private int compareHex(String hex1, String hex2) {
//        long x1 = Long.parseUnsignedLong(hex1, 16);
//        long x2 = Long.parseUnsignedLong(hex2, 16);
        long x1 = fromUnsignedHex(hex1);
        long x2 = fromUnsignedHex(hex2);
        int result = 0;
        if (x1 > x2) {
            result = 1;
        } else if (x1 < x2) {
            result = -1;
        }

        return result;
    }

    private String[] decodeMessage(String str) {
        int length = str.length();
        String ip1 = StringUtils.substring(str, 0, 8);
        String ip2 = "0";

        if (length == 17) {
            ip2 = ip1;
        } else if (length == 19) {
            ip2 = StringUtils.substring(str, 0, 6) + StringUtils.substring(str, 8, 10);
        } else if (length == 21) {
            ip2 = StringUtils.substring(str, 0, 4) + StringUtils.substring(str, 8, 12);
        } else if (length == 23) {
            ip2 = StringUtils.substring(str, 0, 2) + StringUtils.substring(str, 8, 14);
        } else if (length == 25) {
            ip2 = StringUtils.substring(str, 8, 16);
        }

        return new String[]{ip1, ip2};
    }
}

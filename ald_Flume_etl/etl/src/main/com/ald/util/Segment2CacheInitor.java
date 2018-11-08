package com.ald.util;

import com.ald.Contants;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by luanma on 2017/5/14.
 */
public class Segment2CacheInitor implements Contants {
    private static final Logger logger = LoggerFactory.getLogger(Segment2CacheInitor.class);
    public List<Segment> list = new ArrayList<Segment>();

    private Segment2CacheInitor() {
        InputStream inputStream = null;
        BufferedReader bufferedReader = null;
        try {
            inputStream = Segment2CacheInitor.class.getClassLoader().getResourceAsStream("cnipsegment.txt");
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] data = line.split(IP_SEGMENT_CN_DELIMITER);
                String ipBegin = data[0];
                String ipEnd = data[1];
                Segment segment = new Segment();
                segment.begin = IPUtils.ipToLong(ipBegin);
                segment.end = IPUtils.ipToLong(ipEnd);
                list.add(segment);
            }

            logger.info("loaded China ip list");
        } catch (Exception e) {
            logger.error("Found error from file", e);
            list.clear();
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(bufferedReader);
        }
    }

    public static Segment2CacheInitor getInstance() {
        return Segment2CacheInitorHolder.ipFromGeo;
    }

    private static class Segment2CacheInitorHolder {
        private static Segment2CacheInitor ipFromGeo = new Segment2CacheInitor();
    }
}

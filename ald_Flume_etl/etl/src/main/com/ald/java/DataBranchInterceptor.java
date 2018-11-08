package com.ald.java;

import com.ald.FinderManager;
import com.ald.IpLocation;
import com.ald.cache.CacheFactory;
import com.ald.cache.RedisCache;
import com.ald.finder.baidu.BaiduFinder;
import com.ald.finder.geoip.GeoIPFinder;
import com.ald.finder.local.LocalFinder;
import com.ald.finder.qqwry.QQWRYFinder;
import com.ald.util.ConfigUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataBranchInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(DataBranchInterceptor.class);
    private RedisCache redisCache;
    private FinderManager manager;
    private static ConcurrentHashSet concurrentHashSet = new ConcurrentHashSet();
    private final String whiteHeadName = ConfigUtils.get("header.key.name");
    private final String whiteHeadBranch = ConfigUtils.get("header.value.branch");

    private static long TIME_DIFF = 10000L;

    private DataBranchInterceptor(FinderManager manager) {
        this.manager = manager;
    }


    @Override
    public void initialize() {
        logger.info("DataBranchInterceptor initialized.");
        this.redisCache = CacheFactory.getInstances();
        final String redisKey = ConfigUtils.get("ak.whitelist.redis.key");
        new Thread(() -> {
            while (true) {
                try {
                    Set<String> akSet = redisCache.getSet(redisKey);
                    ConcurrentHashSet tempSet = new ConcurrentHashSet(akSet);  //一分钟换一次key
                    concurrentHashSet = tempSet;
                    Thread.sleep(60000);
                    if (Thread.interrupted()) {
                        break;
                    }
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }).start();
    }

    @Override
    public Event intercept(Event event) {
        //不使用他们的方法，用我们自己的
        return null;
    }

    /**
     * 我们特殊的Interceptor
     */
    private JSONObject intercept(JSONObject jsonObject) {
        String ip = jsonObject.getString("client_ip");
        IpLocation location = manager.getLocation(ip);
        if (location == null) {
            location = new IpLocation();
            location.setCountry("中国");
            location.setCity("北京");
            location.setProvince("北京");
            location.setLocationCode("1010101");
        }
        jsonObject.put("country", location.getCountry());
        jsonObject.put("province", location.getProvince());
        jsonObject.put("city", location.getCity());
        return jsonObject;
    }


    private void appendEvent(List<Event> intercepted, Event e) {
        if (e != null) {
            intercepted.add(e);
        }
    }

    /**
     * a1.sources.r1.selector.type = multiplexing
     * a1.sources.r1.selector.header = state
     * a1.sources.r1.selector.mapping.CZ = c1
     * a1.sources.r1.selector.mapping.US = c2 c3
     * a1.sources.r1.selector.default = c4
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(list.size());
        for (Event event : list) {
            String body = new String(event.getBody(), Charsets.UTF_8);
            try {
                JSONObject jsonObject = JSONObject.parseObject(body);
                if (jsonObject.containsKey("ak")) {
                    String ak = jsonObject.getString("ak");
                    if (ak.length() == 32) {
                        if (concurrentHashSet.contains(ak)) {
                            Event headEvent = EventBuilder.withBody(body.getBytes());
                            Map<String, String> headers = headEvent.getHeaders();
                            headers.put(whiteHeadName, whiteHeadBranch);
                            headEvent.setHeaders(headers);
                            appendEvent(intercepted, headEvent);
                        }
                        event.setBody(transform(intercept(jsonObject)).getBytes());
                        appendEvent(intercepted, event);
                    }
                }
            } catch (Exception e) {
                logger.error("json is not valid,event:{}", body, e);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private static final String GEOIP_DB = "geoip";
        private static final String QQWRY_DB = "qqwry";
        private static final String BaiduEnabled = "baidu.geo";

        private String geoipDbFilePath;
        private String qqwryDbFilePath;
        private Boolean baiduGeoEnabled = false;

        @Override
        public Interceptor build() {
            FinderManager manager = new FinderManager();
            manager.setRedisCache(CacheFactory.getInstances());

            manager.getHomeFinders().add(new LocalFinder());

            if (baiduGeoEnabled) {
                manager.getHomeFinders().add(new BaiduFinder());
            }

            if (StringUtils.isNotBlank(qqwryDbFilePath)) {
                try {
                    manager.getHomeFinders().add(new QQWRYFinder(qqwryDbFilePath));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            if (StringUtils.isNotBlank(geoipDbFilePath)) {
                try {
                    manager.getOverseaFinders().add(new GeoIPFinder(geoipDbFilePath));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }

            return new DataBranchInterceptor(manager);
        }

        @Override
        public void configure(Context context) {
            geoipDbFilePath = context.getString(GEOIP_DB);
            qqwryDbFilePath = context.getString(QQWRY_DB);
            baiduGeoEnabled = context.getBoolean(BaiduEnabled, false);

        }
    }

    private String transform(JSONObject json) {
        if (json.containsKey("ak")) {
            String ak = json.getString("ak");
            if (ak.length() != 32) {
                System.out.println("illegal ak");
                return null;
            }
        } else {
            System.out.println("does not contain ak");
            return null;
        }
        if (json.containsKey("server_time")) {
            long current = System.currentTimeMillis();
            long today_start = LocalDateTime.of(LocalDate.now(), LocalTime.MIN).toEpochSecond(OffsetDateTime.now().getOffset());
            long dayStart = today_start * 1000;
            long dayEnd = dayStart + 24 * 3600 * 1000;
            String server_time = json.getString("server_time");
            try {
                String st, et;
                if (json.containsKey("st")) {
                    st = json.getString("st");
                } else if (json.containsKey("ts")) {
                    st = json.getString("ts");
                } else {
                    st = server_time;
                }

                if (!json.containsKey("et")) {
                    et = st;
                } else {
                    et = json.getString("et");
                }
                // ET是上报时间 ，和 Nginx时间对比，做时间校正
                long lst = convertString2Long(server_time);
                if (!(lst >= dayStart && lst < dayEnd)) {
                    lst = current;    //这里需要把昨天数据重置为今天，减少数据误差
                }
//                json.put("server_time", String.valueOf(lst));  //重置服务器时间
                long let = convertString2Long(et);
                long v = lst - let;

                if (Math.abs(v) > TIME_DIFF) {  //误差超过10s
                    let = let + v;
                }
                if (!(let >= dayStart && let < dayEnd)) {
                    let = current;
                }
                long cst = convertString2Long(st);
                if (Math.abs(v) > TIME_DIFF) {
                    cst = cst + v;
                }
                if (!(cst >= dayStart && cst < dayEnd)) {//经过校正还是昨天，重置为今天
                    cst = let;
                }
                json.put("et", let);
                json.put("st", cst);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            System.out.println("does not contain time");
            return null;
        }
        //handle ald_share_src
        if (json.containsKey("ct_path")) {
            String ct_path = json.getString("ct_path");
            if (ct_path.contains("ald_share_src=")) {
                String[] words = ct_path.split("ald_share_src=");
                if (words.length == 2) {
                    json.put("wsr_query_ald_share_src", words[1]);
                }
            }
        }
        //handle avatarURL
        if (json.containsKey("ufo") && json.get("ufo") != null && !json.get("ufo").equals("")) {
            String ufo = json.getString("ufo");
            try {
                JSONObject ufojson = JSON.parseObject(ufo);
                if (ufojson.containsKey("userInfo")) {
                    String userinfo = ufojson.getString("userInfo");
                    JSONObject userinfojson = JSON.parseObject(userinfo);
                    if (userinfojson.containsKey("avatarUrl")) {
                        String avatarUrl = userinfojson.getString("avatarUrl");
                        json.put("avatarUrl", avatarUrl);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // if v=7.0.0 do ifo=>oifo
        if (json.containsKey("v") && json.get("v").toString().compareTo("7.0.0")>=0) {
            if (json.containsKey("oifo")) {
                String oifo = json.getString("oifo");
                json.put("ifo", oifo);
            }
        }

        json.put("ald_intercepted", "true");
        return json.toString();


    }

    /**
     * 处理上报时间，只处理10位数字（需要乘1000）和13位数字（毫秒）
     */
    private long convertString2Long(String st) {
        int pos = st.indexOf(".");
        String sst;
        if (pos > 0) {
            sst = st.substring(0, pos);
            if (sst.length() == 10) {
                return Long.parseLong(sst) * 1000L;
            } else {
                throw new RuntimeException("not valid timestamp!");
            }
        }
        if (st.length() == 13) {
            return Long.parseLong(st);
        } else if (st.length() == 10) {
            return Long.parseLong(st) * 1000L;
        } else {
            throw new RuntimeException("not valid timestamp!");
        }
    }

}

package com.ald.finder.baidu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ald.IpLocation;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by luanma on 2017/5/14.
 */
public class BaiduFinder extends AbstractGeoFromWeb {
    private static final Logger logger = LoggerFactory.getLogger(BaiduFinder.class);

    public BaiduFinder() {
        super();
        logger.info("BaiduFinder inited.");
    }

    @Override
    protected String getSearchUrl(String ip) {
//        String key="RPRx8nVodDQOpZ6VdFS1hNfd";
        String url = "http://api.map.baidu.com/location/ip?ak=RPRx8nVodDQOpZ6VdFS1hNfd&ip=" + ip + "&coor=bd09ll";
        return url;
    }

    @Override
    protected IpLocation parseContent(String content) {
        if (StringUtils.isEmpty(content)) return null;
        try {
            JSONObject jsonObject = JSON.parseObject(content);
            IpLocation ipLocation = new IpLocation();
            ipLocation.setCountry("CN");
            if (jsonObject.getJSONObject("content") != null && jsonObject.getJSONObject("content").getJSONObject("address_detail") != null) {
                String province = jsonObject.getJSONObject("content").getJSONObject("address_detail").getString("province");
                if (StringUtils.isNotEmpty(province)) {
                    ipLocation.setProvince(province);
                }
                String city = jsonObject.getJSONObject("content").getJSONObject("address_detail").getString("city");
                if (StringUtils.isNotEmpty(city)) {
                    ipLocation.setCity(city);
                }
            }
            return ipLocation;
        } catch (Exception e) {
            logger.error("parse content failed", e);
        }
        return null;
    }
}

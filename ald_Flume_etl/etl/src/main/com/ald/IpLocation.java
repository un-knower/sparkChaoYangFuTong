package com.ald;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * Created by luanma on 2017/5/14.
 */
public class IpLocation implements Serializable, Cloneable {
    private String country;
    private String isoCode;
    private String province;
    private String city;
    private String isp;
    private String location;

    private String locationCode;

    public IpLocation() {
    }

    public static IpLocation parseIpLocation(String address, String countryCode) {
        String[] addresses = StringUtils.split(address, ',');
        if (addresses != null && addresses.length >= 2) {
            IpLocation ipLocation = new IpLocation();

            if ("101".equals(countryCode)) {
                ipLocation.setProvince(addresses[0]);
                ipLocation.setCity(addresses[1]);
                ipLocation.setCountry("中国");
                ipLocation.setIsoCode("CN");
            } else {
                ipLocation.setCountry(addresses[0]);
                ipLocation.setCity(addresses[1]);
                ipLocation.setProvince(addresses[0]);
            }

            return ipLocation;
        } else {
            return null;
        }
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getIsoCode() {
        return isoCode;
    }

    public void setIsoCode(String isoCode) {
        this.isoCode = isoCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        if (StringUtils.contains(province, "新疆")) {
            this.province = "新疆";
        } else if (StringUtils.contains(province, "西藏")) {
            this.province = "西藏";
        } else if (StringUtils.contains(province, "广西")) {
            this.province = "广西";
        } else if (StringUtils.contains(province, "宁夏")) {
            this.province = "宁夏";
        } else if (StringUtils.contains(province, "内蒙古")) {
            this.province = "内蒙古";
        } else if (StringUtils.contains(province, "闽")) {
            this.province = "福建";
        } else {
            province = StringUtils.substringBefore(province, "省");
            province = StringUtils.substringBefore(province, "市");
            this.province = province;
        }
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = StringUtils.substringBefore(city, "市");
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
        int position = -1;
        //parse location and separate to  province and city
        if (StringUtils.isNotBlank(location) && !"中国".equals(location)) {
            if (location.contains("上海")) {
                setProvince("上海");
                setCity("上海");
            } else if (location.contains("北京")) {
                setProvince("北京");
                setCity("北京");
            } else if (location.contains("天津")) {
                setProvince("天津");
                setCity("天津");
            } else if (location.contains("重庆")) {
                setProvince("重庆");
                setCity("重庆");
            } else if ((position = location.indexOf("省")) > -1) {
                setProvince(location.substring(0, position + 1));
                int cityPosition = location.indexOf("市");
                if (cityPosition > -1) {
                    setCity(location.substring(position + 1, cityPosition + 1));
                } else {
                    setCity(location.substring(position + 1));
                }
            } else if ((position = location.indexOf("自治区")) > -1) {
                setProvince(location.substring(0, position + 3));
                int cityPosition = location.indexOf("市");
                if (cityPosition > -1) {
                    setCity(location.substring(position + 3, cityPosition + 1));
                } else {
                    setCity(location.substring(position + 1));
                }
            } else {
                setProvince(location);
                setCity(location);
            }
        }
    }

    public String getLocationCode() {
        return locationCode;
    }

    public void setLocationCode(String locationCode) {
        this.locationCode = locationCode;
    }

    public String getCountryCode() {
        return StringUtils.substring(locationCode, 0, 3);
    }

    public String getProvinceCode() {
        return StringUtils.substring(locationCode, 0, 5);
    }

    public String getCityCode() {
        return StringUtils.substring(locationCode, 0, 7);
    }

    @Override
    public String toString() {
        return String.format("IpLocation[country='%s', province='%s', city='%s', address='%s', countryCode='%s']",
                country, province, city, location, getCountryCode());
    }

    @Override
    public IpLocation clone() {
        IpLocation other = new IpLocation();
        other.location = location;
        other.country = country;
        other.province = province;
        other.city = city;
        other.isoCode = isoCode;
        other.isp = isp;
        other.locationCode = locationCode;
        return other;
    }
}

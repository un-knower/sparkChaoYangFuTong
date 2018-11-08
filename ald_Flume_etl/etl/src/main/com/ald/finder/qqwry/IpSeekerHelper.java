package com.ald.finder.qqwry;

import com.ald.IpLocation;
import com.ald.util.IPUtils;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by luanma on 2017/5/14.
 */
public class IpSeekerHelper {
    static final int RecordLength = 7;
    static final byte RedirectMode1 = 0x01;
    static final byte RedirectMode2 = 0x02;

    private final ByteBuffer buffer;
    private final IpSeeker seeker;

    IpSeekerHelper(IpSeeker seeker) {
        this.buffer = seeker.buffer;
        this.seeker = seeker;
    }

    /**
     * 计算中间位置的偏移
     */
    static int calcMiddleOffset(final int begin, final int end) {
        final int records = ((end - begin) / RecordLength) >> 1;
        return begin + ((records == 0) ? 1 : records) * RecordLength;
    }

    static int compare(byte[] ip, byte[] begin) {
        for (int i = 0, x, y; i < 4; i++) {
            x = ip[i];
            y = begin[i];
            if ((x & 0xFF) > (y & 0xFF)) {
                return 1;
            } else if ((x ^ y) == 0) {
                continue;
            } else {
                return -1;
            }
        }
        return 0;
    }

    IpLocation getLocation(int offset) {
        if (offset == -1) {
            return null;
        }
        buffer.position(offset + 4);
        IpLocation ipLocation = new IpLocation();
        String content = "";
        switch (buffer.get()) {
            case RedirectMode1:            // Read CountryOffset & Set Position
                buffer.position(offset = readInt3());
                final String country;
                switch (buffer.get()) {
                    case RedirectMode2:
                        country = readString(readInt3());
                        buffer.position(offset + 4);
                        break;
                    default:
                        country = readString(offset);
                        break;
                }
                //ipLocation.setCountry(country);
                content = country;
                if (StringUtils.isNotEmpty(content)) {
                    String[] s = proviceSplit(content);
                    if (s != null) {
                        ipLocation.setProvince(s[0]);
                        ipLocation.setCity(s[1]);
                    } else
                        ipLocation.setLocation(content);
                }
                //ipLocation.setLocation(readArea(buffer.position()));
                return ipLocation;
            case RedirectMode2:
//                ipLocation.setCountry(readString(readInt3()));
                content = readString(readInt3());
                if (StringUtils.isNotEmpty(content)) {
                    String[] s = proviceSplit(content);
                    if (s != null) {
                        ipLocation.setProvince(s[0]);
                        ipLocation.setCity(s[1]);
                    } else
                        ipLocation.setLocation(content);
                }
                //ipLocation.setLocation(readArea(offset + 8));
                return ipLocation;
            default:
                content = readString(buffer.position() - 1);
                if (StringUtils.isNotEmpty(content)) {
                    String[] s = proviceSplit(content);
                    if (s != null) {
                        ipLocation.setProvince(s[0]);
                        ipLocation.setCity(s[1]);
                    } else
                        ipLocation.setLocation(content);
                }
                //ipLocation.setCountry();
                //ipLocation.setLocation(readArea(buffer.position()));
                return ipLocation;
        }
    }

    /**
     * 定位IP的绝对偏移
     */
    int locateOffset(final byte[] address) {
        switch (compare(address, readIP(seeker.offsetBegin))) {
            case -1:
                return -1;
            case 0:
                return seeker.offsetBegin;
        }
        int middleOffset = 0;
        for (int begin = seeker.offsetBegin, end = seeker.offsetEnd; begin < end; ) {
            switch (compare(address, readIP(middleOffset = calcMiddleOffset(begin, end)))) {
                case 1:
                    begin = middleOffset;
                    break;
                case -1:
                    if (middleOffset == end) {
                        middleOffset = (end -= RecordLength);
                    } else {
                        end = middleOffset;
                    }
                    break;
                case 0:
                    return readInt3(middleOffset + 4);
            }
        }
        middleOffset = readInt3(middleOffset + 4);
        switch (compare(address, readIP(middleOffset))) {
            case -1:
            case 0:
                return middleOffset;
            default:
                return -1;
        }
    }

    private String readArea(int offset) {
        buffer.position(offset);
        switch (buffer.get()) {
            case RedirectMode1:
            case RedirectMode2:
                offset = readInt3(offset + 1);
                return (offset != 0) ? readString(offset) : "";
            default:
                return readString(offset);
        }
    }

    private int readInt3() {
        return buffer.getInt() & 0x00FFFFFF;
    }

    int readInt3(int offset) {
        buffer.position(offset);
        return buffer.getInt() & 0x00FFFFFF;
    }

    byte[] readIP(int offset) {
        buffer.position(offset);
        return IPUtils.toBytes(buffer.getInt());
    }

    private String[] proviceSplit(String content) {
        int position = -1;
        String proviceName = "";
        if (content.indexOf("内蒙") == 0) {
            position = 3;
            proviceName = "内蒙古自治区";
        } else if (content.indexOf("新疆") == 0) {
            position = 2;
            proviceName = "新疆维吾尔自治区";
        } else if (content.indexOf("广西") == 0) {
            position = 2;
            proviceName = "广西壮族自治区";
        } else if (content.indexOf("西藏") == 0) {
            position = 2;
            proviceName = "西藏自治区";
        } else if (content.indexOf("宁夏") == 0) {
            position = 2;
            proviceName = "宁夏回族自治区";
        }
        if (position > 0) {
            String[] s = new String[2];
            s[0] = proviceName;
            s[1] = content.substring(position, content.length());
            return s;
        }
        return null;
    }

    private String readString(int offset) {
        buffer.position(offset);
        final byte[] buf = new byte[0xFF];
        offset = -1;
        while ((buf[++offset] = buffer.get()) != 0) ;
        return (offset != 0) ? new String(buf, 0, offset, Charset.forName("GBK")) : null;
    }
}

package com.ald.finder.qqwry;

import com.ald.IpLocation;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by luanma on 2017/5/14.
 */
public class IpSeekerSimpleCache extends IpSeeker {
    private final Map<byte[], IpLocation> cache = new Hashtable();

    public IpSeekerSimpleCache(Path path) throws IOException {
        super(path);
    }

    public IpSeekerSimpleCache(byte[] bytes) throws IOException {
        super(bytes);
    }

    @Override
    public synchronized IpLocation getLocation(byte ip1, byte ip2, byte ip3, byte ip4) {
        final byte[] ip = {ip1, ip2, ip3, ip4};
        if (cache.containsKey(ip)) {
            return cache.get(ip);
        } else {
            return cache.put(ip, super.getLocation(ip));
        }
    }
}

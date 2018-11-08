package com.ald.finder.qqwry;

import com.ald.IpLocation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by luanma on 2017/5/14.
 */
public class IpSeeker {
    final ByteBuffer buffer;
    final IpSeekerHelper helper;
    final int offsetBegin, offsetEnd;

    public IpSeeker(Path path) throws IOException {
        if (Files.exists(path)) {
            buffer = ByteBuffer.wrap(Files.readAllBytes(path));
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            offsetBegin = buffer.getInt(0);
            offsetEnd = buffer.getInt(4);
            if (offsetBegin == -1 || offsetEnd == -1) {
                throw new IllegalArgumentException("File Format Error");
            }
            helper = new IpSeekerHelper(this);
        } else {
            throw new FileNotFoundException();
        }
    }

    public IpSeeker(byte[] bytes) throws IOException {
        buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        offsetBegin = buffer.getInt(0);
        offsetEnd = buffer.getInt(4);
        if (offsetBegin == -1 || offsetEnd == -1) {
            throw new IllegalArgumentException("File Format Error");
        }
        helper = new IpSeekerHelper(this);
    }

    public IpLocation getLocation(final byte ip1, final byte ip2, final byte ip3, final byte ip4) {
        return getLocation(new byte[]{ip1, ip2, ip3, ip4});
    }

    protected final IpLocation getLocation(final byte[] ip) {
        return helper.getLocation(helper.locateOffset(ip));
    }

    public IpLocation getLocation(final InetAddress address) {
        return getLocation(address.getAddress());
    }
}

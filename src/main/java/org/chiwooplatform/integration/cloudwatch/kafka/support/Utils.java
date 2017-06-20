package org.chiwooplatform.integration.cloudwatch.kafka.support;

import java.util.UUID;

import java.nio.ByteBuffer;

public class Utils {

    public static final String uuid() {
        return UUID.randomUUID().toString();
    }

    public static long tXID() {
        return ByteBuffer.wrap( uuid().getBytes() ).asLongBuffer().get();
    }

    public static String getTXID() {
        return Long.toString( tXID() );
    }
}

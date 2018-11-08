package com.ald.stat;

import com.esotericsoftware.kryo.Kryo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;

public class RegisterKypoSerializer implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ConsumerRecord.class);
    }
}

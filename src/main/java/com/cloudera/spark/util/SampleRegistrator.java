package com.cloudera.spark.util;

/**
 * Created by spabba on 3/13/14.
 */

import com.cloudera.spark.common.STBEvent;
import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;

public class SampleRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(STBEvent.class);
    }
}
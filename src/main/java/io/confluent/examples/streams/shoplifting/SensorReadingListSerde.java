package io.confluent.examples.streams.shoplifting;

import io.confluent.examples.streams.shoplifting.SensorReading;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

public class SensorReadingListSerde implements Serde<List<SensorReading>> {

    @Override
    public Serializer<List<SensorReading>> serializer() {
        return new SensorReadingListSerializer();
    }

    @Override
    public Deserializer<List<SensorReading>> deserializer() {
        return new SensorReadingListDeserializer();
    }
}

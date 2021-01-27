package io.confluent.examples.streams.shoplifting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class SensorReadingSerde implements Serde<SensorReading> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<SensorReading> serializer() {
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };
    }

    @Override
    public Deserializer<SensorReading> deserializer() {
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, SensorReading.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        };
    }
}

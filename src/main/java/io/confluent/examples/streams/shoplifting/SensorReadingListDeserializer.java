package io.confluent.examples.streams.shoplifting;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Stream processing unit test of {@link SequenceExample}, using TopologyTestDriver.
 * <p>
 * See {@link SequenceExample} for further documentation.
 */

public class SensorReadingListDeserializer implements Deserializer<List<SensorReading>> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<SensorReading> deserialize(String topic, byte[] data) {
        TypeReference<List<SensorReading>> sensorReadingListTypeReference =
                new TypeReference<List<SensorReading>>() {
                    @Override
                    public Type getType() {
                        return super.getType();
                    }
                };

        try {
            return objectMapper.readValue(data, sensorReadingListTypeReference);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

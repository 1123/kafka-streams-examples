package io.confluent.examples.streams.shoplifting;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;

/**
 * Stream processing unit test of {@link SequenceExample}, using TopologyTestDriver.
 * <p>
 * See {@link SequenceExample} for further documentation.
 */

public class SensorReadingListSerializer implements Serializer<List<SensorReading>> {

    ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, List<SensorReading> data) {
        return objectMapper.writeValueAsBytes(data);
    }
}

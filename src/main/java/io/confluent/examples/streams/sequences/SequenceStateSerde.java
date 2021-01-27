package io.confluent.examples.streams.sequences;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.shoplifting.SensorReading;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import scala.collection.Seq;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

public class SequenceStateSerde<T> implements Serde<SequenceState<T>> {

    @Override
    public Serializer<SequenceState<T>> serializer() {
        return new Serializer<SequenceState<T>>() {

            final ObjectMapper objectMapper = new ObjectMapper();

            @SneakyThrows
            @Override
            public byte[] serialize(String topic, SequenceState<T> sequenceState) {
                return objectMapper.writeValueAsBytes(sequenceState);
            }

        };
    }

    @Override
    public Deserializer<SequenceState<T>> deserializer() {
        return new Deserializer<SequenceState<T>>() {

            final ObjectMapper objectMapper = new ObjectMapper();

            @SneakyThrows
            @Override
            public SequenceState<T> deserialize(String topic, byte[] data) {
                return objectMapper.readValue(data, SequenceState.class);
            }
        };
    }
}

package io.confluent.examples.streams.sorting.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MessageSerializer implements Serializer<Message> {

    private static final Logger log = LoggerFactory.getLogger(MessageSerializer.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String s, Message message) {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            log.warn(e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public void close() {
        // nothing to do
    }
}

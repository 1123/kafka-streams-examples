package io.confluent.examples.streams.sorting.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {

    private static final Logger log = LoggerFactory.getLogger(MessageSerializer.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // nothing to do
    }

    @Override
    public Message deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Message.class);
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to do
    }
}

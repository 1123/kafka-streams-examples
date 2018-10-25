package io.confluent.examples.streams.sorting.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.sorting.stream.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

class MessageGenerator {
  private static final Logger log = LoggerFactory.getLogger(MessageGenerator.class);

  private ObjectMapper objectMapper = new ObjectMapper();

  private String serialize(Message m) {
    try {
      return objectMapper.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage());
    }
    return null;
  }

  List<String> generateOutOfOrderMessages(final int count) {
    Random r = new Random();
    List<Message> messages = new ArrayList<>();
    for(int i = 0; i < count; i++) {
      messages.add(new Message(i * 10L + r.nextInt(20)));
    }
    return messages.stream().map(this::serialize).filter(Objects::nonNull).collect(Collectors.toList());
  }

}

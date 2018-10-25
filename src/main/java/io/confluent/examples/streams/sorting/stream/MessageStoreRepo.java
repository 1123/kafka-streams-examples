package io.confluent.examples.streams.sorting.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class MessageStoreRepo {

  private static final Logger log = LoggerFactory.getLogger(MessageStoreRepo.class);

  private MessageStoreRepo() {}

  private static ObjectMapper objectMapper = new ObjectMapper();

  static MessageStore fromStore(KeyValueStore<Integer, String> store) {
    try {
      return (store.get(1) == null) ? new MessageStore() : objectMapper.readValue(store.get(1), MessageStore.class);
    } catch (IllegalArgumentException | IOException e) {
      log.warn(e.getMessage());
      return new MessageStore();
    }
  }

  static void persist(MessageStore messageStore, KeyValueStore<Integer, String> store) {
    try {
      store.put(1, objectMapper.writeValueAsString(messageStore));
    } catch (JsonProcessingException e) {
      log.warn(e.getMessage());
    }
  }

}

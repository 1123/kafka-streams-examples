package io.confluent.examples.streams.sorting.stream;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.stream.Collectors;

class ReorderPunctuator implements Punctuator {

  private final ProcessorContext context;
  private final KeyValueStore<Integer, String> store;

  ReorderPunctuator(KeyValueStore<Integer, String> store, ProcessorContext context) {
    this.store = store;
    this.context = context;
  }

  @Override
  public void punctuate(long timestamp) {
    MessageStore messageStore = MessageStoreRepo.fromStore(store);
    List<Message> oldMessages = messageStore.getMessages().stream().filter(m -> m.getTimestamp() + 1000 < timestamp).collect(Collectors.toList());
    messageStore.getMessages().removeAll(oldMessages);
    oldMessages.forEach(m -> context.forward(m.getTimestamp(), m));
    MessageStoreRepo.persist(messageStore, store);
  }

}

package io.confluent.examples.streams.sorting.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

class MessageReorderTransformer implements Transformer<Integer, Message, KeyValue<Long, Message>> {

  private String storeName;

  private KeyValueStore<Integer, String> store;

  MessageReorderTransformer(String stateStore) {
    this.storeName = stateStore;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(ProcessorContext context) {
    store = (KeyValueStore<Integer, String>) context.getStateStore(storeName);
    context.schedule(10000, PunctuationType.STREAM_TIME, new ReorderPunctuator(store, context));
  }

  @Override
  public KeyValue<Long, Message> transform(Integer key, Message message)  {
    MessageStore messageStore = MessageStoreRepo.fromStore(store);
    messageStore.getMessages().add(message);
    MessageStoreRepo.persist(messageStore, store);
    return null;
  }

  @Override
  public void close() {
    // No need to close anything: https://stackoverflow.com/questions/49181359/kafka-streams-closing-processors-state-store
  }
}

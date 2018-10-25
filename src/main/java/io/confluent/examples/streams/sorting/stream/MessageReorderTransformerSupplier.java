package io.confluent.examples.streams.sorting.stream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

class MessageReorderTransformerSupplier implements TransformerSupplier<Integer, Message, KeyValue<Long, Message>> {

  private final String storeName;

  MessageReorderTransformerSupplier(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public Transformer<Integer, Message, KeyValue<Long, Message>> get() {
    return new MessageReorderTransformer(storeName);
  }
}

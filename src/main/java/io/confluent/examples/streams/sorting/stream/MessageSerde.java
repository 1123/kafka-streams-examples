package io.confluent.examples.streams.sorting.stream;

import org.apache.kafka.common.serialization.Serdes;

public class MessageSerde extends Serdes.WrapperSerde<Message> {
    MessageSerde() {
        super(new MessageSerializer(), new MessageDeserializer());
    }
}

package io.confluent.examples.streams.sorting.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class SortingOutOfOrderEventsStream {

    MessageDeserializer messageDeserializer = new MessageDeserializer();

    void createStream(StreamsBuilder builder) {
        final KStream<Integer, String> messages = builder.stream(SortingOutOfOrderEventsExample.INPUT_TOPIC);
        messages.print(Printed.toSysOut());
        KStream<Long, Message> sorted = messages
                // the topic is ignored
                .mapValues(value -> messageDeserializer.deserialize("foo", value.getBytes()))
                .transform(
                        new MessageReorderTransformerSupplier(SortingOutOfOrderEventsExample.STATE_STORE_NAME),
                        SortingOutOfOrderEventsExample.STATE_STORE_NAME
                );
        sorted.to(SortingOutOfOrderEventsExample.OUTPUT_TOPIC, Produced.with(Serdes.Long(), new MessageSerde()));
    }

}

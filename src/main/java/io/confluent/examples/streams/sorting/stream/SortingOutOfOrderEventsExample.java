/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.sorting.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.sorting.driver.SortingOutOfORderEventsExampleDriver;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Demo App for sorting out of order events and publish to a new topic.
 *
 * <p>
 *
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 *
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * </p>
 * <p>
 * 2) Create the input/intermediate/output topics used by this example:
 * <pre>
 *  {@code
 *  $ bin/kafka-topics --create --topic out-of-order-messages --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 *  $ bin/kafka-topics --create --topic sorted-messages --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 *  }</pre>
 * </p>
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * </p>
 * <p>
 * 4) Generate some out of order events to the input topic (e.g. via {@link SortingOutOfORderEventsExampleDriver}). The
 * already running example application (step 3) will read these events and sort them by the timestamps contained in the
 * payloads. See the generated messages and note that they are out of order:
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic out-of-order-messages  --bootstrap-server localhost:9092 --from-beginning --key-deserializer org.apache.kafka.common.serialization.IntegerDeserializer --property print.key=true
 * }
 * </pre>
 * </p>
 * <p>
 * 5) Watch the reordered messages.
 * </p>
 */

public class SortingOutOfOrderEventsExample {

    private static final Logger log = LoggerFactory.getLogger(SortingOutOfOrderEventsExample.class);
    static final String STATE_STORE_NAME = "reorder-state-store";
    public static final String OUTPUT_TOPIC = "sorted-messages";
    public static final String INPUT_TOPIC = "out-of-order-messages";


    private static Properties streamsConfiguration(String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pair-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pair-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamsConfiguration;
    }


    private static ObjectMapper objectMapper = new ObjectMapper();

    private static StoreBuilder sortedMessagesStoreBuilder() {
        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore(STATE_STORE_NAME);
        return new KeyValueStoreBuilder<>(
                store,
                new Serdes.IntegerSerde(),
                new Serdes.StringSerde(),
                Time.SYSTEM
        );
    }

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(sortedMessagesStoreBuilder());

        SortingOutOfOrderEventsStream sortingOutOfOrderEventsStream = new SortingOutOfOrderEventsStream();

        sortingOutOfOrderEventsStream.createStream(builder);

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration(bootstrapServers));

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

class SortingOutOfOrderEventsStream {

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


class MessageSerde extends Serdes.WrapperSerde<Message> {
    MessageSerde() {
        super(new MessageSerializer(), new MessageDeserializer());
    }
}

class MessageSerializer implements Serializer<Message> {

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


class MessageDeserializer implements Deserializer<Message> {

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



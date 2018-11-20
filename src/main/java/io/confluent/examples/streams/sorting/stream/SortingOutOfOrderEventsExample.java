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

import io.confluent.examples.streams.sorting.driver.SortingOutOfORderEventsExampleDriver;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

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
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(sortedMessagesStoreBuilder());

        SortingOutOfOrderEventsStream sortingOutOfOrderEventsStream = new SortingOutOfOrderEventsStream();
        sortingOutOfOrderEventsStream.createStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration(bootstrapServers));

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}



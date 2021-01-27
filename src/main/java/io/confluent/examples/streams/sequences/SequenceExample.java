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
package io.confluent.examples.streams.sequences;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

import java.util.Arrays;
import java.util.Properties;

/**
 */
public class SequenceExample {

  static final String SENSOR_READINGS_TOPIC = "sensor-readings";
  static final String SHOPLIFTS_TOPIC = "shoplifts";

  /**
   * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
   */
  public static void main(final String[] args) {
    final Properties streamsConfiguration = getStreamsConfiguration();

    final StreamsBuilder builder = new StreamsBuilder();
    createTopology(builder);
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.cleanUp();
    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  /**
   * Configure the Streams application.
   *
   * Various Kafka Streams related settings are defined here such as the location of the target Kafka cluster to use.
   * Additionally, you could also define Kafka Producer and Kafka Consumer settings when needed.
   *
   * @return Properties getStreamsConfiguration
   */
  public static Properties getStreamsConfiguration() {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sequence-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sequence-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    // For illustrative purposes we disable record caches.
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    return streamsConfiguration;
  }

  /**
   * Define the processing topology.
   *
   * @param builder StreamsBuilder to use
   */
  static void createTopology(final StreamsBuilder builder) {
    builder.addStateStore(
            new KeyValueStoreBuilder<String, SequenceState<String>>(
                    new RocksDbKeyValueBytesStoreSupplier("sensor-readings", true),
                    new Serdes.StringSerde(),
                    new MySequenceStateSerde<>(),
                    Time.SYSTEM
            )
    );

    final KStream<String, String> readings = builder.stream(SENSOR_READINGS_TOPIC);

    KStream<String, SequenceState<String>> shoplifts = readings
            .transform(new SequenceTransformerSupplier<>(
                    Arrays.asList(
                            SeqElement.<String, String>builder().predicate(keyValue -> keyValue.value.equals("SHELF")).build(),
                            SeqElement.<String, String>builder().predicate(keyValue -> keyValue.value.equals("COUNTER")).negated(true).build(),
                            SeqElement.<String, String>builder().predicate(keyValue -> keyValue.value.equals("EXIT")).build()
                    )
            ), "sensor-readings");

    shoplifts.to(SHOPLIFTS_TOPIC, Produced.with(new Serdes.StringSerde(), new MySequenceStateSerde<>()));
  }

}


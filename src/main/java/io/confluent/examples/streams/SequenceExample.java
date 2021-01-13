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
package io.confluent.examples.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.common.utils.TestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 */
public class SequenceExample {

  static final String inputTopic = "streams-plaintext-input";
  static final String outputTopic = "streams-plaintext-output";

  /**
   * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
   */
  public static void main(final String[] args) {
    final Properties streamsConfiguration = getStreamsConfiguration("localhost:9092");

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
   * @param bootstrapServers Kafka cluster address
   * @return Properties getStreamsConfiguration
   */
  static Properties getStreamsConfiguration(final String bootstrapServers) {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sequence-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sequence-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
            new KeyValueStoreBuilder<>(
                    new RocksDbKeyValueBytesStoreSupplier("sensor-readings", true),
                    new Serdes.StringSerde(),
                    new Serdes.StringSerde(),
                    Time.SYSTEM
            )
    );

    final KStream<String, String> textLines = builder.stream(inputTopic);

    textLines
      .transform(new SequenceTransformerSupplier(), "sensor-readings")
      .to(outputTopic);
  }

}

class SequenceTransformer implements Transformer<String, String, KeyValue<String, String>> {

  private KeyValueStore<String, String> store;
  private ProcessorContext context;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.store = context.getStateStore("sensor-readings");
  }

  @Override
  public KeyValue<String, String> transform(String key, String value) {
    String old = store.get(key);
    SensorData sensorData;
    try {
      if (old == null) {
        sensorData = new SensorData();
      } else {
        sensorData = objectMapper.readValue(old, SensorData.class);
      }
      switch (value) {
        case "SHELF":
          sensorData.setLastShelfTimestamp(context.timestamp());
          store.put(key, objectMapper.writeValueAsString(sensorData));
          return null;
        case "COUNTER":
          sensorData.setLastCounterTimestamp(context.timestamp());
          store.put(key, objectMapper.writeValueAsString(sensorData));
          return null;
        case "EXIT":
          if (sensorData.getLastExitTimestamp() > 0L) return null; // The product had already left the shop before.
          sensorData.setLastExitTimestamp(context.timestamp());
          if (sensorData.getLastShelfTimestamp() == 0L) {
            throw new RuntimeException("Invalid sequence of events: the product has never been seen on a shelf. ");
          }
          store.put(key, objectMapper.writeValueAsString(sensorData));
          if (sensorData.getLastCounterTimestamp() == 0L) {
            // Shop lifting detected -- the product has not been seen at the counter.
            return new KeyValue<>(key, objectMapper.writeValueAsString(sensorData));
          } else {
            return null;
          }
        default:
          throw new RuntimeException("Invalid sensor reading value");
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void close() {

  }
}

class SequenceTransformerSupplier implements TransformerSupplier<String, String, KeyValue<String,String>> {

  @Override
  public Transformer<String, String, KeyValue<String, String>> get() {
    return new SequenceTransformer();
  }
}

@EqualsAndHashCode
@Data
@AllArgsConstructor
@NoArgsConstructor
class SensorData {

  private long lastShelfTimestamp;
  private long lastCounterTimestamp;
  private long lastExitTimestamp;

}
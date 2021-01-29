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
package io.confluent.examples.streams.shoplifting;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 */
public class ShopLiftingJoinExample {

  public static final String SENSOR_READINGS = "SENSOR_READINGS";
  public static final String SHOPLIFTS = "SHOP_LIFTS";

  public static void createTopology(StreamsBuilder builder) {
    final KStream<String, SensorReading> sensorReadings =
            builder.stream(SENSOR_READINGS, Consumed.with(new Serdes.StringSerde(), new SensorReadingSerde()));
    KStream<String, SensorReading> shelfReadings = sensorReadings.filter((key, value) -> value.getType().equals("SHELF"));
    KStream<String, SensorReading> counterReadings = sensorReadings.filter((key, value) -> value.getType().equals("COUNTER"));
    KStream<String, SensorReading> exitReadings = sensorReadings.filter((key, value) -> value.getType().equals("EXIT"));
    KStream<String, List<SensorReading>> shelfAndExitsStream =
            shelfReadings.join(
                    exitReadings,
                    Arrays::asList,
                    JoinWindows.of(Duration.ofMinutes(1)),
                    StreamJoined.with(new Serdes.StringSerde(), new SensorReadingSerde(), new SensorReadingSerde())
            );
    KStream<String, List<SensorReading>> shelfAndCounterStream =
            shelfReadings.join(
                    counterReadings,
                    Arrays::asList,
                    JoinWindows.of(Duration.ofMinutes(1)),
                    StreamJoined.with(new Serdes.StringSerde(), new SensorReadingSerde(), new SensorReadingSerde())
            );
    KStream<String, List<SensorReading>> shelfAndCounterAndExits = shelfAndExitsStream.leftJoin(
            shelfAndCounterStream,
            (v1, v2) -> {
              if (v2 == null) { return  Arrays.asList(v1.get(0), v1.get(1)); }
              else { return Arrays.asList(v1.get(0), v1.get(1), v2.get(0), v2.get(1)); }
            },
            JoinWindows.of(Duration.ofMinutes(1)),
            StreamJoined.with(new Serdes.StringSerde(), new SensorReadingListSerde(), new SensorReadingListSerde())
    );
    shelfAndCounterAndExits.filter((key,value) -> value.size() == 2)
            .to(SHOPLIFTS, (Produced.with(new Serdes.StringSerde(), new SensorReadingListSerde())));
  }
}


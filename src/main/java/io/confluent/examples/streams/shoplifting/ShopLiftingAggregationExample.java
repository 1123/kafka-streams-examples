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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

@Slf4j
public class ShopLiftingAggregationExample {

  public static final String SENSOR_READINGS = "SENSOR_READINGS";
  public static final String SHOPLIFTS = "SHOP_LIFTS";

  public static void createTopology(StreamsBuilder builder) {
    final KStream<String, SensorReading> sensorReadings =
            builder.stream(SENSOR_READINGS, Consumed.with(new Serdes.StringSerde(), new SensorReadingSerde()));
    sensorReadings
            .groupByKey(Grouped.with(new Serdes.StringSerde(), new SensorReadingSerde()))
            .aggregate(
              ArrayList::new,
              ShopLiftingAggregationExample::transform,
              Materialized.with(new Serdes.StringSerde(), new SensorReadingListSerde())
            )
            .toStream()
            .filter((key, value) -> (
                    value.size() == 2 &&
                            value.get(0).type.equals("SHELF") &&
                            value.get(1).type.equals("EXIT"))
            )
            .to(SHOPLIFTS);
  }

  public static List<SensorReading> transform(String key, SensorReading sensorReading, List<SensorReading> aggregate) {
    if (sensorReading.type.equals("SHELF")) {
      // multiple SHELF readings may occur. Only keep the last one.
      return Collections.singletonList(sensorReading);
    }
    if (sensorReading.type.equals("COUNTER")) {
      if (aggregate.size() == 1) {
        // only keep the first COUNTER reading.
        // TODO: maybe we should better keep the last, as for shelf readings?
        aggregate.add(sensorReading);
      }
      return aggregate;
    }
    if (sensorReading.type.equals("EXIT")) {
      if (aggregate.size() == 2 && aggregate.get(0).type.equals("SHELF") && aggregate.get(1).type.equals("COUNTER")) {
        aggregate.add(sensorReading);
      } else {
        if (aggregate.size() == 1 && aggregate.get(0).type.equals("SHELF")) {
          aggregate.add(sensorReading);
          log.info("Shoplifting detected: {}", aggregate.toString());
        }
        // ignore the case where shoplifting has already been detected, a valid checkout has been detected,
        // or an exit reading without a shelf reading.
      }
      return aggregate;
    }
    throw new RuntimeException(String.format("unexpected sensorReading: %s", sensorReading));
  }
}


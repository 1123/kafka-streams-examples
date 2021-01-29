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

import io.confluent.examples.streams.sequences.SequenceStateSerde;
import io.confluent.examples.streams.sequences.SeqElement;
import io.confluent.examples.streams.sequences.SequenceState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;
import java.util.function.Function;

@Slf4j
public class GenericSequencesNegationByAggregationExample {

  public static final String SENSOR_READINGS = "SENSOR_READINGS";
  public static final String SHOPLIFTS = "SHOP_LIFTS";

  public static void createTopology(StreamsBuilder builder) {
    final KStream<String, SensorReading> sensorReadings =
            builder.stream(SENSOR_READINGS, Consumed.with(new Serdes.StringSerde(), new SensorReadingSerde()));
    sensorReadings
            .groupByKey(Grouped.with(new Serdes.StringSerde(), new SensorReadingSerde()))
            .aggregate(SequenceState::new,
                    new AggregateSequenceTransformer<>(
                            Arrays.asList(
                                    new SeqElement<>(false, keyValue -> keyValue.value.type.equals("SHELF")),
                                    new SeqElement<>(true, keyValue -> keyValue.value.type.equals("COUNTER")),
                                    new SeqElement<>(false, keyValue -> keyValue.value.type.equals("EXIT"))
                            )
                    ),
                    Materialized.with(new Serdes.StringSerde(), new SequenceStateSerde<>())
            )
            .toStream().filter(((key, value) -> value.isMatched())).to(SHOPLIFTS);
  }

}

@Slf4j
class AggregateSequenceTransformer<K,V> implements Aggregator<K, V, SequenceState<V>> {

  private final List<SeqElement<K, V>> elements;
  public AggregateSequenceTransformer(List<SeqElement<K,V>> elements) {
    this.elements = elements;
  }

  @Override
  public SequenceState<V> apply(K key, V value, SequenceState<V> state) {
      Function<KeyValue<K, V>, Boolean> predicate = elements.get(state.getPosition()).getPredicate();
      if (elements.get(state.getPosition()).isNegated()) {
        if (predicate.apply(new KeyValue<>(key, value))) {
          return new SequenceState<>();
        }
        Function<KeyValue<K, V>, Boolean> nextPredicate = elements.get(state.getPosition() + 1).getPredicate();
        if (nextPredicate.apply(new KeyValue<>(key, value))) {
          state.advance(2);
          state.getValues().add(null);
          state.getValues().add(value);
          if (state.getPosition() == elements.size()) {
            state.setMatched(true);
          }
        }
      } else {
        if (predicate.apply(new KeyValue<>(key, value))) {
          state.advance(1);
          state.getValues().add(value);
          if (state.getPosition() == elements.size()) {
            state.setMatched(true);
          }
        }
      }
    return state;
  }
}

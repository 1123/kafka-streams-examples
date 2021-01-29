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
                                    SeqElement.<String, SensorReading>builder()
                                            .predicate(keyValue -> keyValue.value.type.equals("SHELF")).build(),
                                    SeqElement.<String, SensorReading>builder().predicate(
                                            keyValue -> keyValue.value.type.equals("COUNTER")).negated(true).build(),
                                    SeqElement.<String, SensorReading>builder().predicate(
                                            keyValue -> keyValue.value.type.equals("EXIT")).build()
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
          log.info("Negated element matched");
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
          // negated element did not match, but the following positive element matched;
        }
        // negated element did not match, and the following positive element matched neither.
        // position does not advance.
      } else { // not negated
        if (predicate.apply(new KeyValue<>(key, value))) {
          state.advance(1);
          state.getValues().add(value);
          if (state.getPosition() == elements.size()) {
            // last element. Match found. Start from scratch.
            // positive element matched. End of sequence reached.
            state.setMatched(true);
          }
          // positive element matched, but end of sequence was not reached.
        }
        // positive element did not match. Position is not advanced.
      }
    return state;
  }
}

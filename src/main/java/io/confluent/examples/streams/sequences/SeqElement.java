package io.confluent.examples.streams.sequences;

import lombok.Builder;
import org.apache.kafka.streams.KeyValue;

import java.util.function.Function;

@Builder
public class SeqElement<K, V> {
    boolean negated;
    Function<KeyValue<K, V>, Boolean> predicate;
}

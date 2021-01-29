package io.confluent.examples.streams.sequences;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.streams.KeyValue;

import java.util.function.Function;

@Getter
@AllArgsConstructor
public class SeqElement<K, V> {
    boolean negated;
    Function<KeyValue<K, V>, Boolean> predicate;
}

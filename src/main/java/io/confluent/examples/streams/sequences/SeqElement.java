package io.confluent.examples.streams.sequences;

import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.streams.KeyValue;

import java.util.function.Function;

@Builder
@Getter
public class SeqElement<K, V> {
    boolean negated;
    Function<KeyValue<K, V>, Boolean> predicate;
}

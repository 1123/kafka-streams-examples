package io.confluent.examples.streams.sequences;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

import java.util.List;

public class SequenceTransformerSupplier<K, V> implements TransformerSupplier<K, V, KeyValue<K, SequenceState<V>>> {

    private final List<SeqElement<K, V>> elements;

    public SequenceTransformerSupplier(List<SeqElement<K, V>> elements) {
        this.elements = elements;
    }

    @Override
    public Transformer<K, V, KeyValue<K, SequenceState<V>>> get() {
        return new SequenceTransformer<>(elements);
    }
}

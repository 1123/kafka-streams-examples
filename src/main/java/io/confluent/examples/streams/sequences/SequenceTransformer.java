package io.confluent.examples.streams.sequences;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.function.Function;

public class SequenceTransformer<K, V> implements Transformer<K, V, KeyValue<K, SequenceState<V>>> {

    private final List<SeqElement<K, V>> elements;
    private KeyValueStore<K, SequenceState<V>> store;
    private ProcessorContext context;

    public SequenceTransformer(List<SeqElement<K, V>> elements) {
        this.elements = elements;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = context.getStateStore("sensor-readings");
    }

    @Override
    public KeyValue<K, SequenceState<V>> transform(K key, V value) {
        SequenceState<V> state = store.get(key);
        try {
            if (state == null) {
                // first message that is being processed
                state = new SequenceState<>();
            }
            state.timestamp = context.timestamp();
            Function<KeyValue<K, V>, Boolean> predicate = elements.get(state.position).predicate;
            if (elements.get(state.position).negated) {
                if (predicate.apply(new KeyValue<>(key, value))) {
                    // a negated element matched. Therefore we start from scratch.
                    // no match found
                    store.put(key, null);
                    return null; // negated element matched
                }
                Function<KeyValue<K, V>, Boolean> nextPredicate = elements.get(state.position + 1).predicate;
                if (nextPredicate.apply(new KeyValue<>(key, value))) {
                    state.position += 2; // skip over the negated element and the following element
                    // TODO: what about two consecutive negations? Is there a use case for this?
                    // state.values.add("NEGATED ELEMENT -- NO MATCH");
                    state.values.add(null);
                    state.values.add(value);
                    store.put(key, state);
                    if (state.position == elements.size()) {
                        // last element. Match found. Start from scratch.
                        store.put(key, null);
                        // Emit the match downstream.
                        // negated element did not match, but the following positive element matched;
                        // end of sequence reached.
                        return new KeyValue<>(key, state);
                    }
                    // negated element did not match, but the following positive element matched;
                    // end of sequence not yet reached.
                    return null;
                }
                // negated element did not match, and the following positive element matched neither.
                // position does not advance.
                return null;
            } else { // not negated
                if (predicate.apply(new KeyValue<>(key, value))) {
                    state.position++;
                    state.values.add(value);
                    store.put(key, state);
                    if (state.position == elements.size()) {
                        // last element. Match found. Start from scratch.
                        store.put(key, null);
                        // positive element matched. End of sequence reached.
                        // Emit the match downstream.
                        return new KeyValue<>(key, state);
                    }
                    // positive element matched, but end of sequence was not reached.
                    return null;
                }
                // positive element did not match. Position is not advanced.
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {

    }
}

package io.confluent.examples.streams.sequences;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Data
public class SequenceState<V> {

    int position = 0;
    long timestamp;
    boolean matched = false;
    List<V> values = new ArrayList<>();

    public void advance(int i) {
        this.position += i;
    }

}

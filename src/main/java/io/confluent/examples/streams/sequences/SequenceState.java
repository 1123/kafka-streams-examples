package io.confluent.examples.streams.sequences;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class SequenceState<V> {

    int position = 0;
    long timestamp;
    List<V> values = new ArrayList<>();
}

package io.confluent.examples.streams.shoplifting;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    String type;
    long timestamp;
}

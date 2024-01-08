/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import com.sun.source.tree.Tree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_connectors {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    ;

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);
        final byte semicolon = ";".getBytes(StandardCharsets.UTF_8)[0];
        final byte new_line = System.lineSeparator().getBytes(StandardCharsets.UTF_8)[0];

        var size = 128 * 1024 * 1024;
        var intermediates = new ArrayList<TreeMap<String, MeasurementAggregator>>();

        try (FileChannel file = FileChannel.open(Paths.get(FILE))) {
            long position = 0;
            int count = 0;
            Semaphore waiter = new Semaphore(0);
            do {
                final var length = Math.min(size, file.size() - position);
                final var buffer = file.map(FileChannel.MapMode.READ_ONLY, position, length);
                final var target = new TreeMap<String, MeasurementAggregator>();
                intermediates.add(target);

                Thread.ofPlatform().name("reader-" + count).start(() -> {
                    var nameBuffer = ByteBuffer.allocate(1024);
                    var valueBuffer = ByteBuffer.allocate(1024);

                    var inName = true;
                    while (buffer.hasRemaining()) {
                        var next = buffer.get();

                        if (next == new_line) {
                            final var name = new String(nameBuffer.array(), 0, nameBuffer.position());
                            final var value = new String(valueBuffer.array(), 0, valueBuffer.position());

                            target.compute(name, (key, current) -> {
                                try {
                                    var parsedValue = Double.parseDouble(value);
                                    if (current == null) {
                                        var agg = new MeasurementAggregator();
                                        agg.min = parsedValue;
                                        agg.max = parsedValue;
                                        agg.sum = parsedValue;
                                        agg.count = 1;
                                        return agg;
                                    }

                                    current.min = Math.min(current.min, parsedValue);
                                    current.max = Math.min(current.max, parsedValue);
                                    current.sum += parsedValue;
                                    current.count += 1;
                                }
                                catch (Throwable t) {
                                    System.out.printf("Error: '%s' -> '%s'%n", key, value);
                                }

                                return current;
                            });

                            inName = true;
                            nameBuffer.clear();
                            valueBuffer.clear();
                            continue;
                        }

                        // check semicolon
                        if (next == semicolon) {
                            inName = false;
                            continue;
                        }

                        // if not semicolon
                        if (inName) {
                            nameBuffer.put(next);
                        }
                        else {
                            valueBuffer.put(next);
                        }
                    }

                    waiter.release();
                });

                count += 1;
                position += length;
            } while (position < file.size());

            waiter.acquire(count);
        }

        System.out.println(intermediates.size());

        // Collector<String, MeasurementAggregator, ResultRow> collector = Collector.of(
        // MeasurementAggregator::new,
        // (a, m) -> {
        // var value = Double.parseDouble(m.substring(m.indexOf(';') + 1));

        // a.min = Math.min(a.min, value);
        // a.max = Math.max(a.max, value);
        // a.sum += value;
        // a.count++;
        // },
        // (agg1, agg2) -> {
        // var res = new MeasurementAggregator();
        // res.min = Math.min(agg1.min, agg2.min);
        // res.max = Math.max(agg1.max, agg2.max);
        // res.sum = agg1.sum + agg2.sum;
        // res.count = agg1.count + agg2.count;

        // return res;
        // },
        // agg -> {
        // return new ResultRow(agg.min, agg.sum / agg.count, agg.max);
        // });

        // Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
        // .parallel()
        // // .map(l -> new Measurement(l.split(";")))
        // .collect(groupingBy(input -> input.substring(0, input.indexOf(';')), collector)));

        // System.out.println(measurements);
    }
}

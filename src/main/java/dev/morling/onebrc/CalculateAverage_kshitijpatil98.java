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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class CalculateAverage_kshitijpatil98 {
    public static void main(String[] args) {
        try (Stream<String> lines = Files.lines(Paths.get(MEASUREMENTS_PATH))) {
            final ConcurrentHashMap<String, Stats> stats = new ConcurrentHashMap<>(10000);
            lines
                    .parallel()
                    .map(line -> line.split(";", 2))
                    .filter(parts -> parts.length == 2)
                    .map(parts -> new Measurement(parts[0], Double.parseDouble(parts[1])))
                    .forEach(measurement -> stats.compute(measurement.name, (_, value) -> {
                        if (value == null)
                            return new Stats(measurement);
                        else {
                            return new Stats(
                                    Math.min(value.min, measurement.temperature),
                                    Math.max(value.max, measurement.temperature),
                                    (value.average * value.countSoFar + measurement.temperature) / (value.countSoFar + 1),
                                    value.countSoFar + 1);
                        }
                    }));
            final var sortedStats = new TreeMap<>(stats);
            System.out.println(sortedStats);
        }
        catch (IOException e) {
            System.err.printf("File not found: %s", MEASUREMENTS_PATH);
            System.exit(1);
        }
    }

    private static final String MEASUREMENTS_PATH = "./measurements.txt";

    record Measurement(String name, double temperature) {
    }

    record Stats(double min, double max, double average, int countSoFar) {
        public Stats(Measurement measurement) {
            this(measurement.temperature, measurement.temperature, measurement.temperature, 1);
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min, average, max);
        }
    }
}

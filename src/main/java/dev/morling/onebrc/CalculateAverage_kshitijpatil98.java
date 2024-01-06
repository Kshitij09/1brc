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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CalculateAverage_kshitijpatil98 {
    public static void main(String[] args) {
        try (Stream<String> lines = Files.lines(Paths.get(MEASUREMENTS_PATH))) {
            var stationMeasurements = lines
                    .parallel()
                    .map(CalculateAverage_kshitijpatil98::parseMeasurement)
                    .collect(
                            Collectors.toConcurrentMap(
                                    m -> m.name,
                                    Stats::new,
                                    Stats::merge,
                                    ConcurrentSkipListMap::new));
            System.out.println(stationMeasurements);
        }
        catch (IOException e) {
            System.err.printf("File not found: %s", MEASUREMENTS_PATH);
            System.exit(1);
        }
    }

    private static final String MEASUREMENTS_PATH = "./measurements.txt";
    private static final char MEASUREMENT_DELIMITER = ';';
    private static final char DECIMAL_DOT = '.';

    record Measurement(String name, double temperature) {
    }

    private static Measurement parseMeasurement(String line) {
        int delimiterIndex = line.indexOf(MEASUREMENT_DELIMITER);
        var stationName = line.substring(0, delimiterIndex);
        var temperature = parseTemperature(line.substring(delimiterIndex + 1));
        // System.out.printf("parsed %s and %s", stationName, temperature);
        return new Measurement(stationName, temperature);
    }

    private static double parseTemperature(String tempString) {
        var isNegative = tempString.charAt(0) == '-';
        var digitIndex = (isNegative) ? 1 : 0;
        double temperature = 0.0;
        for (; digitIndex < tempString.length(); digitIndex++) {
            if (tempString.charAt(digitIndex) == DECIMAL_DOT)
                continue;
            temperature = temperature * 10 + (tempString.charAt(digitIndex) - '0');
        }
        return (isNegative) ? -temperature : temperature;
    }

    record Stats(double min, double max, double average, int countSoFar) {
        public Stats(final Measurement measurement) {
            this(measurement.temperature, measurement.temperature, measurement.temperature, 1);
        }

        public Double sum() {
            return average * countSoFar;
        }

        public static Stats merge(final Stats first, final Stats second) {
            return new Stats(
                    Math.min(first.min, second.min),
                    Math.max(first.max, second.max),
                    (first.sum() + second.sum()) / (first.countSoFar + second.countSoFar),
                    first.countSoFar + second.countSoFar
            );
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", (min / 10f), (average / 10f), (max / 10f));
        }
    }
}

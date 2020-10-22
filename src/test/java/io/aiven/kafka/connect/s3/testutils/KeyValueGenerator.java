/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.s3.testutils;

import java.util.Iterator;

public class KeyValueGenerator implements Iterable<KeyValueMessage> {

    public final int numPartitions;
    public final int numEpochs;
    public final IndexesToString keyGenerator;
    public final IndexesToString valueGenerator;

    public KeyValueGenerator(final int numPartitions,
                             final int numEpochs,
                             final IndexesToString keyGenerator,
                             final IndexesToString valueGenerator) {
        this.numPartitions = numPartitions;
        this.numEpochs = numEpochs;
        this.keyGenerator = keyGenerator;
        this.valueGenerator = valueGenerator;
    }

    @Override
    public Iterator<KeyValueMessage> iterator() {
        return new Iterator<>() {
            int partition = 0;
            int epoch = 0;
            int currIdx = 0;

            @Override
            public boolean hasNext() {
                return epoch < numEpochs;
            }

            @Override
            public KeyValueMessage next() {
                final KeyValueMessage msg =
                    new KeyValueMessage(keyGenerator.generate(partition, epoch, currIdx),
                                         valueGenerator.generate(partition, epoch, currIdx),
                                         partition,
                                         currIdx,
                                         epoch
                    );
                currIdx += 1;
                partition += 1;
                if (partition >= numPartitions) {
                    epoch += 1;
                    partition = 0;
                }
                return msg;
            }
        };
    }
}

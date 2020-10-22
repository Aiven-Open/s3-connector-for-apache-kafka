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

public class KeyValueMessage {
    public final String key;
    public final String value;
    public final int partition;
    public final int idx;
    public final int epoch;

    public KeyValueMessage(final String key,
                           final String value,
                           final int partition,
                           final int idx,
                           final int epoch) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.idx = idx;
        this.epoch = epoch;
    }
}

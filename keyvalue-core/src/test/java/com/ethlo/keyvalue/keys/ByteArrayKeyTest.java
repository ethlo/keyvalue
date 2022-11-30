package com.ethlo.keyvalue.keys;

/*-
 * #%L
 * Key/Value API
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;


class ByteArrayKeyTest
{
    @Test
    void testHashCode()
    {
        final byte[] keyData = "eatme".getBytes(StandardCharsets.UTF_8);
        final ByteArrayKey b1 = new ByteArrayKey(keyData);
        final ByteArrayKey b2 = new ByteArrayKey(keyData);
        assertThat(b1.hashCode()).isEqualTo(b2.hashCode());
    }

    @Test
    void testEquals()
    {
        final ByteArrayKey b1 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
        final ByteArrayKey b2 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
        final ByteArrayKey c1 = new ByteArrayKey("drinkme".getBytes(StandardCharsets.UTF_8));

        assertThat(b1).isEqualTo(b2);
        assertThat(b2).isEqualTo(b1);
        assertThat(c1).isNotEqualTo(b1).isNotEqualTo(b2);
    }

    @Test
    void testSerializeAndDeserialize() throws IOException, ClassNotFoundException
    {
        // Serialize
        final ByteArrayKey b1 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(b1);
        oout.flush();

        // Deserialize
        final ByteArrayKey b2 = (ByteArrayKey) new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray())).readObject();
        assertThat(b1.getByteArray()).isEqualTo(b1.getByteArray());
        assertThat(b1).isEqualTo(b2);
    }
}

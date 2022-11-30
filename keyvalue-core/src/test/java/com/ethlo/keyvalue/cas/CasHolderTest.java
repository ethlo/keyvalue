package com.ethlo.keyvalue.cas;

/*-
 * #%L
 * Key-Value - Core
 * %%
 * Copyright (C) 2013 - 2022 Morten Haraldsen (ethlo)
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class CasHolderTest
{
    @Test
    void testEquals()
    {
        final byte[] keyA = new byte[]{1, 2, 3};
        final byte[] valueA = new byte[]{5, 6, 7};
        final CasHolder<byte[], byte[], Integer> a = new CasHolder<>(0, keyA, valueA);

        final byte[] keyB = new byte[]{1, 2, 4};
        final byte[] valueB = new byte[]{5, 6, 8};
        final CasHolder<byte[], byte[], Integer> b = new CasHolder<>(0, keyB, valueB);

        Assertions.assertThat(a).isEqualTo(a).isEqualTo(new CasHolder<>(0, keyA, valueA));
        Assertions.assertThat(b).isEqualTo(b).isNotEqualTo(a);
    }
}

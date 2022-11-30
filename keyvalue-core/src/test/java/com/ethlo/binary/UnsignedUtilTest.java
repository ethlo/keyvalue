package com.ethlo.binary;

/*-
 * #%L
 * keyvalue-binary
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UnsignedUtilTest
{
    @Test
    void encodeByte()
    {
        final short value = (short) UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(result).isEqualTo(new byte[]{-1});
    }

    @Test
    void encodeShort()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 2);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF});
    }

    @Test
    void encode24Bit()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 3);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    }

    @Test
    void encode32Bit()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 4);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    }

    @Test
    void decodeUnsignedInt()
    {
        final byte[] data = new byte[]{-1, -1, -1, -1};
        final long result = UnsignedUtil.decodeUnsignedInt(data, 0);
        assertThat(result).isEqualTo(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
    }

    @Test
    void decodeUnsignedZeroLength()
    {
        final byte[] data = new byte[]{-1};
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.decodeUnsigned(data, 0, 0));
    }

    @Test
    void decodeUnsignedMoreThan64BitLength()
    {
        final byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.decodeUnsigned(data, 0, 9));
    }

    @Test
    void decodeUnsignedOutOfArrayBounds()
    {
        final byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> UnsignedUtil.decodeUnsigned(data, 5, 6));
    }

    @Test
    void decodeUnsignedShort()
    {
        final byte[] data = new byte[]{-1, -1};
        final long result = UnsignedUtil.decodeUnsignedShort(data, 0);
        assertThat(result).isEqualTo(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE);
    }

    @Test
    void encodeUnsigned()
    {
        for (int length = 1; length <= 8; length++)
        {
            final byte[] value = UnsignedUtil.encodeUnsigned(255, length);
            assertThat(value.length).isEqualTo(length);
        }
    }

    @Test
    void getMaxSizeZeroBytes()
    {
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.getMaxSize(0));
    }

    @Test
    void getMaxSizeMoreThan8Bytes()
    {
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.getMaxSize(9));
    }

    @Test
    void getMaxSize()
    {
        assertThat(UnsignedUtil.getMaxSize(1)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(2)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(3)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(4)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(5)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_40BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(6)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_48BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(7)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_56BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(8)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_63BIT_INT_VALUE);
    }

    @Test
    void encodeUnsignedByte()
    {
        final short value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(data.length).isEqualTo(1);
        assertThat(UnsignedUtil.decodeUnsignedByte(data, 0)).isEqualTo(value);
    }

    @Test
    void encodeTooLargeValueForBits()
    {
        final int value = 70_000;
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.encodeUnsignedShort(value));
    }

    @Test
    void encodeUnsignedShort()
    {
        final int value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedShort(value);
        assertThat(data.length).isEqualTo(2);
        assertThat(UnsignedUtil.decodeUnsignedShort(data, 0)).isEqualTo(value);
    }

    @Test
    void encodeUnsignedIntWithReuiredNumberOfBytes()
    {
        final long value = 998281928292021093L;
        final int bytesRequired = UnsignedUtil.getRequiredBytesForUnsigned(value);
        assertThat(bytesRequired).isEqualTo(8);
        final byte[] data = UnsignedUtil.encodeUnsigned(value, bytesRequired);
        assertThat(UnsignedUtil.decodeUnsigned(data, 0, bytesRequired)).isEqualTo(value);
    }

    @Test
    void decodeUnsignedByte()
    {
        final short value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(data.length).isEqualTo(1);
        assertThat(UnsignedUtil.decodeUnsignedByte(data, 0)).isEqualTo(value);
    }

    @Test
    void decodeUnsigned()
    {
        final long value = 998281928292021093L;
        final int bytesRequired = UnsignedUtil.getRequiredBytesForUnsigned(value);
        assertThat(bytesRequired).isEqualTo(8);
        final byte[] data = UnsignedUtil.encodeUnsigned(value, bytesRequired);
        assertThat(UnsignedUtil.decodeUnsigned(data, 0, bytesRequired)).isEqualTo(value);
    }

    @Test
    void getRequiredBytesForUnsigned()
    {
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE, 1);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE, 2);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE, 3);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE, 4);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_40BIT_INT_VALUE, 5);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_48BIT_INT_VALUE, 6);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_56BIT_INT_VALUE, 7);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_63BIT_INT_VALUE, 8);
    }

    private void checkAtBoundary(final long value, final int byteCountAssert)
    {
        assertThat(UnsignedUtil.getRequiredBytesForUnsigned(value)).isEqualTo(byteCountAssert);
        assertThat(UnsignedUtil.getRequiredBytesForUnsigned(value - 1)).isEqualTo(byteCountAssert);
    }

    @Test
    void getRequiredBytesForUnsignedNegative()
    {
        final long value = -1;
        Assertions.assertThrows(IllegalArgumentException.class, () -> UnsignedUtil.getRequiredBytesForUnsigned(value));
    }

    @Test
    void encodeUnsignedInt()
    {
        final byte[] data = UnsignedUtil.encodeUnsignedInt(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
        assertThat(data).isEqualTo(new byte[]{-1, -1, -1, -1});
    }
}

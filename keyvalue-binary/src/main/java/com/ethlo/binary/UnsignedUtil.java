package com.ethlo.binary;

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

public class UnsignedUtil
{
    public static final long MAX_UNSIGNED_64BIT_INT_VALUE = (long) Math.pow(2.0D, 64.0D) - 1L;
    public static final long MAX_UNSIGNED_56BIT_INT_VALUE = (long) Math.pow(2.0D, 56.0D) - 1L;
    public static final long MAX_UNSIGNED_48BIT_INT_VALUE = (long) Math.pow(2.0D, 48.0D) - 1L;
    public static final long MAX_UNSIGNED_40BIT_INT_VALUE = (long) Math.pow(2.0D, 40.0D) - 1L;
    public static final long MAX_UNSIGNED_32BIT_INT_VALUE = 4_294_967_295L;
    public static final long MAX_UNSIGNED_24BIT_INT_VALUE = 16_777_215L;
    public static final long MAX_UNSIGNED_16BIT_INT_VALUE = 65_535L;
    public static final long MAX_UNSIGNED_8BIT_INT_VALUE = 255L;
    public static final int BITS_IN_BYTE = 8;
    public static final int BYTES_IN_LONG = 8;

    private static final long[] bytesToMaxValue = new long[]
            {
                    MAX_UNSIGNED_8BIT_INT_VALUE, MAX_UNSIGNED_16BIT_INT_VALUE, MAX_UNSIGNED_24BIT_INT_VALUE, MAX_UNSIGNED_32BIT_INT_VALUE,
                    MAX_UNSIGNED_40BIT_INT_VALUE, MAX_UNSIGNED_48BIT_INT_VALUE, MAX_UNSIGNED_56BIT_INT_VALUE, MAX_UNSIGNED_64BIT_INT_VALUE
            };

    public static byte[] encodeUnsigned(long value, int length)
    {
        assertValueFits(value, length, getMaxSize(length));
        final byte[] result = new byte[length];
        for (int i = length - 1; i >= 0; --i)
        {
            result[i] = (byte) ((int) (value & 255L));
            value >>= 8;
        }
        return result;
    }

    public static long getMaxSize(final int byteCount)
    {
        if (byteCount < 0L)
        {
            throw new IllegalArgumentException("An unsigned number requires at least 1 byte");
        }

        if (byteCount > 8)
        {
            throw new IllegalArgumentException("Number is too large for an unsigned integer: " + byteCount);
        }

        return bytesToMaxValue[byteCount - 1];
    }

    public static byte[] encodeUnsignedByte(short value)
    {
        return encodeUnsigned(value, 1);
    }

    public static byte[] encodeUnsignedShort(int value)
    {
        return encodeUnsigned(value, 2);
    }

    public static byte[] encodeUnsignedInt(long value)
    {
        return encodeUnsigned(value, 4);
    }

    public static short decodeUnsignedByte(byte[] data, int offset)
    {
        return (short) decodeUnsigned(data, offset, 1);
    }

    public static int decodeUnsignedShort(byte[] data, int offset)
    {
        return (int) decodeUnsigned(data, offset, 2);
    }

    public static long decodeUnsignedInt(byte[] data, int offset)
    {
        return decodeUnsigned(data, offset, 4);
    }

    public static long decodeUnsigned(byte[] data, int offset, int length)
    {
        if (length < 1)
        {
            throw new IllegalArgumentException("length cannot be less than 1 byte");
        }

        if (length > BYTES_IN_LONG)
        {
            throw new IllegalArgumentException("length cannot be more than 8 bytes, got " + length);
        }

        if (offset + length > data.length)
        {
            throw new ArrayIndexOutOfBoundsException("Cannot access bytes " + offset + "-" + (offset + length - 1) + " as array length is only " + data.length);
        }

        long l = 0;
        for (int i = 0; i < length - 1; i++)
        {
            final byte b = data[i + offset];
            l |= ((long) b) & MAX_UNSIGNED_8BIT_INT_VALUE;
            l <<= BITS_IN_BYTE;
        }
        final byte b = data[offset + length - 1];
        l |= ((long) b) & MAX_UNSIGNED_8BIT_INT_VALUE;
        return l;
    }

    public static int getRequiredBytesForUnsigned(long value)
    {
        if (value < 0L)
        {
            throw new IllegalArgumentException("An unsigned number must be 0 or positive");
        }
        else if (value <= MAX_UNSIGNED_8BIT_INT_VALUE)
        {
            return 1;
        }
        else if (value <= MAX_UNSIGNED_16BIT_INT_VALUE)
        {
            return 2;
        }
        else if (value <= MAX_UNSIGNED_24BIT_INT_VALUE)
        {
            return 3;
        }
        else if (value <= MAX_UNSIGNED_40BIT_INT_VALUE)
        {
            return 5;
        }
        else if (value <= MAX_UNSIGNED_48BIT_INT_VALUE)
        {
            return 6;
        }
        else if (value <= MAX_UNSIGNED_56BIT_INT_VALUE)
        {
            return 7;
        }
        else if (value <= MAX_UNSIGNED_64BIT_INT_VALUE)
        {
            return 8;
        }
        throw new IllegalArgumentException("Number is too large for an unsigned integer: " + value);
    }

    private static void assertValueFits(long value, int bytes, long maxValue)
    {
        if (value > maxValue)
        {
            throw new IllegalArgumentException("Value too large for " + bytes + " bytes: " + value + ". Maximum value allowed is " + maxValue);
        }
    }
}

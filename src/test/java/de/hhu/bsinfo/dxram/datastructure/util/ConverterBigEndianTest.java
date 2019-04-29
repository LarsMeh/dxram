package de.hhu.bsinfo.dxram.datastructure.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConverterBigEndianTest {

    @Test
    public void testEndianness() {
        long l_value = 4;
        int i_value = 4;
        short s_value = 4;
        byte[] l_bigEndian = {0, 0, 0, 0, 0, 0, 0, 4};
        byte[] i_bigEndian = {0, 0, 0, 4};
        byte[] s_bigEndian = {0, 4};

        Assert.assertArrayEquals(l_bigEndian, ConverterBigEndian.longToByteArray(l_value));
        Assert.assertArrayEquals(i_bigEndian, ConverterBigEndian.intToByteArray(i_value));
        Assert.assertArrayEquals(s_bigEndian, ConverterBigEndian.shortToByteArray(s_value));
    }

    @Test
    public void testShort() {
        short value = Short.MAX_VALUE;
        short value2 = Short.MIN_VALUE;

        short result = ConverterBigEndian.byteArrayToShort(ConverterBigEndian.shortToByteArray(value));
        short result2 = ConverterBigEndian.byteArrayToShort(ConverterBigEndian.shortToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testInt() {
        int value = Integer.MAX_VALUE;
        int value2 = Integer.MIN_VALUE;

        int result = ConverterBigEndian.byteArrayToInt(ConverterBigEndian.intToByteArray(value));
        int result2 = ConverterBigEndian.byteArrayToInt(ConverterBigEndian.intToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testLong() {
        long value = Long.MAX_VALUE;
        long value2 = Long.MIN_VALUE;

        long result = ConverterBigEndian.byteArrayToLong(ConverterBigEndian.longToByteArray(value));
        long result2 = ConverterBigEndian.byteArrayToLong(ConverterBigEndian.longToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }
}

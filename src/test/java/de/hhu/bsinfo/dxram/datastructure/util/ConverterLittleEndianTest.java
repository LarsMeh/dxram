package de.hhu.bsinfo.dxram.datastructure.util;

import org.junit.Assert;
import org.junit.Test;

public class ConverterLittleEndianTest {

    @Test
    public void testEndianness() {
        long l_value = 4;
        int i_value = 4;
        short s_value = 4;
        byte[] l_littleEndian = {4, 0, 0, 0, 0, 0, 0, 0};
        byte[] i_littleEndian = {4, 0, 0, 0};
        byte[] s_littleEndian = {4, 0};

        Assert.assertArrayEquals(l_littleEndian, ConverterLittleEndian.longToByteArray(l_value));
        Assert.assertArrayEquals(i_littleEndian, ConverterLittleEndian.intToByteArray(i_value));
        Assert.assertArrayEquals(s_littleEndian, ConverterLittleEndian.shortToByteArray(s_value));
    }

    @Test
    public void testShort() {
        short value = Short.MAX_VALUE;
        short value2 = Short.MIN_VALUE;

        short result = ConverterLittleEndian.byteArrayToShort(ConverterLittleEndian.shortToByteArray(value));
        short result2 = ConverterLittleEndian.byteArrayToShort(ConverterLittleEndian.shortToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testInt() {
        int value = Integer.MAX_VALUE;
        int value2 = Integer.MIN_VALUE;

        int result = ConverterLittleEndian.byteArrayToInt(ConverterLittleEndian.intToByteArray(value));
        int result2 = ConverterLittleEndian.byteArrayToInt(ConverterLittleEndian.intToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testLong() {
        long value = Long.MAX_VALUE;
        long value2 = Long.MIN_VALUE;

        long result = ConverterLittleEndian.byteArrayToLong(ConverterLittleEndian.longToByteArray(value));
        long result2 = ConverterLittleEndian.byteArrayToLong(ConverterLittleEndian.longToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

}
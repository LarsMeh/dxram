package de.hhu.bsinfo.dxram.datastructure.util;

import org.junit.Assert;
import org.junit.Test;

public class ConverterTest {

    @Test
    public void testShort() {
        short value = Short.MAX_VALUE;
        short value2 = Short.MIN_VALUE;

        short result = Converter.byteArrayToShort(Converter.shortToByteArray(value));
        short result2 = Converter.byteArrayToShort(Converter.shortToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testInt() {
        int value = Integer.MAX_VALUE;
        int value2 = Integer.MIN_VALUE;

        int result = Converter.byteArrayToInt(Converter.intToByteArray(value));
        int result2 = Converter.byteArrayToInt(Converter.intToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }

    @Test
    public void testLong() {
        long value = Long.MAX_VALUE;
        long value2 = Long.MIN_VALUE;

        long result = Converter.byteArrayToLong(Converter.longToByteArray(value));
        long result2 = Converter.byteArrayToLong(Converter.longToByteArray(value2));

        Assert.assertEquals(value, result);
        Assert.assertEquals(value2, result2);
    }
}

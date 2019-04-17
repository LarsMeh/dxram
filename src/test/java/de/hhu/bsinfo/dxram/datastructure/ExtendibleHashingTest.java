package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxram.datastructure.util.Converter;
import de.hhu.bsinfo.dxram.datastructure.util.ExtendibleHashing;
import org.junit.Assert;
import org.junit.Test;

public class ExtendibleHashingTest {

    @Test
    public void nBitCompareForByteArray() {
        boolean result;

        int tmp = Integer.parseInt("000 0000 0000 0000 1000 1000 1000 1010".replace(" ", ""), 2);
        byte[] suspect = Converter.intToByteArray(tmp);


        result = ExtendibleHashing.nBitCompare(suspect, 2);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.nBitCompare(suspect, 4);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.nBitCompare(suspect, 8);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.nBitCompare(suspect, 16);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.nBitCompare(suspect, 32);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.nBitCompare(suspect, 5);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.nBitCompare(suspect, 33);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.nBitCompare(suspect, 1);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.nBitCompare(suspect, -1);
        Assert.assertEquals(false, result);
    }

    @Test
    public void compareBitForDepth() {
        boolean result;

        int tmp = Integer.parseInt("0000 0000 0000 0000 1000 1000 1000 1010".replace(" ", ""), 2);
        byte[] suspect = Converter.intToByteArray(tmp);


        result = ExtendibleHashing.compareBitForDepth(suspect, 31);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 29);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 25);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 21);
        Assert.assertEquals(true, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 20);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 22);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 32);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 1);
        Assert.assertEquals(false, result);

        result = ExtendibleHashing.compareBitForDepth(suspect, 33);
        Assert.assertEquals(false, result);
    }

    @Test
    public void extendableHashing() {
        int result;

        int tmp = Integer.parseInt("0100 0000 0000 0000 1000 1000 1000 1010".replace(" ", ""), 2);
        byte[] suspect = Converter.intToByteArray(tmp);

        result = ExtendibleHashing.extendibleHashing(suspect, (short) 0);
        Assert.assertEquals(-1, result);

        result = ExtendibleHashing.extendibleHashing(suspect, (short) 1);
        Assert.assertEquals(0, result);

        result = ExtendibleHashing.extendibleHashing(suspect, (short) 2);
        Assert.assertEquals(1, result);

        result = ExtendibleHashing.extendibleHashing(suspect, (short) 3);
        Assert.assertEquals(2, result);

        result = ExtendibleHashing.extendibleHashing(suspect, (short) 4);
        Assert.assertEquals(4, result);

    }
}

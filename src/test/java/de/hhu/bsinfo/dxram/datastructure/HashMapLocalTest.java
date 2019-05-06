package de.hhu.bsinfo.dxram.datastructure;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HashMapLocalTest {

    @Test
    public void validParameter() {
        final List<Short> validList = new ArrayList<>(1);
        validList.add((short) 10);
        final List<Short> invalidList = new ArrayList<>(1);

        Assert.assertTrue(HashMap.assertInitialParameter("abcde", 10, validList, -1, (short) 4, (short) 4, (byte) 1));
        Assert.assertFalse(HashMap.assertInitialParameter("abcde", 10, invalidList, -1, (short) 4, (short) 4, (byte) 1));
    }
}

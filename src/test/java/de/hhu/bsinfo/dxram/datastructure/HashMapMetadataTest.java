package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import org.junit.Test;

public class HashMapMetadataTest {


    @Test
    public void runStructuredTest() {
        DXMem memory = new DXMem((short) 0xAACD, 10000000);

        HashMapMetadataStructuredTest structuredTest = new HashMapMetadataStructuredTest(memory);


        System.out.println("INIT");
        structuredTest.init(10L, 20L, 100, (short) -1, 30352112, (byte) 1);

        System.out.println("INCREMENT AND DECREMENT");
        structuredTest.incrementSize(200, 100);
        structuredTest.incrementSize(200, 0);
        structuredTest.decrementSize(100, 100);
        structuredTest.decrementSize(100, 0);

        System.out.println("END");
        structuredTest.end();

    }
}

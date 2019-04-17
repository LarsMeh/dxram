package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class NodePoolTest {

    @Test
    public void runStructuredTest() {

        ArrayList<Short> list = new ArrayList<>();

        for (short i = 0; i < 10; i++) {
            list.add(i);
        }

        System.out.println(Arrays.toString(list.toArray()));

        DXMem memory = new DXMem((short) 0xAACD, 10000000);
        NodePoolStructuredTest structuredTest = new NodePoolStructuredTest(memory, list);

        System.out.println("INIT");
        structuredTest.init(10);
        System.out.println("RANDOM ACCESS");
        structuredTest.randomAccessVisualize(1);
        System.out.println("END");
        structuredTest.end();

        System.out.println();

        System.out.println("INIT");
        structuredTest.init(10);
        System.out.println("RANDOM ACCESS");
        structuredTest.randomAccessVisualize(20);
        System.out.println("END");
        structuredTest.end();


    }
}

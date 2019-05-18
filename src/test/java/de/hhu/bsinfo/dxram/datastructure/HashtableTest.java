package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.skema.Skema;
import org.junit.Test;

public class HashtableTest {

    @Test
    public void runStructuredTest() {
        DXMem memory = new DXMem((short) 0xAACD, 100000000);
        int initialSize = (int) Math.pow(2, 2) * Long.BYTES + 2;
        long cid = memory.create().create(initialSize);

        HashtableStructuredTest structuredTest = new HashtableStructuredTest(memory, cid);

        System.out.println("INIT");
        structuredTest.init(new long[]{0xAAAA}, initialSize);

        System.out.println("SPLIT");
        structuredTest.split(0xAAAA, 0xCCCC, 1);

        System.out.println("LOOKUP");
        structuredTest.lookup(Skema.serialize(0xAAAA), 0);
        structuredTest.lookup(Skema.serialize(0xAAAA), 1);
        structuredTest.lookup(Skema.serialize(0xCCCC), 2);
        structuredTest.lookup(Skema.serialize(0xCCCC), 3);

        System.out.println("SPLIT");
        structuredTest.split(0xAAAA, 0xBBBB, 0);

        System.out.println("SPLIT");
        structuredTest.split(0xCCCC, 0xDDDD, 3);

        System.out.println("LOOKUP");
        structuredTest.lookup(Skema.serialize(0xAAAA), 0);
        structuredTest.lookup(Skema.serialize(0xBBBB), 1);
        structuredTest.lookup(Skema.serialize(0xCCCC), 2);
        structuredTest.lookup(Skema.serialize(0xDDDD), 3);


        String expect;
        boolean exc = false;

        System.out.println("RESIZE");
        expect = "*********************************************************************************************HashtableChunkSize:66Bytes***HEADER***Depth:3***DATA***0xAAAA<==2or00xAAAA<==10or10xBBBB<==18or20xBBBB<==26or30xCCCC<==34or40xCCCC<==42or50xDDDD<==50or60xDDDD<==58or7*********************************************************************************************";
        try {
            structuredTest.resize(expect, 3);
        } catch (RuntimeException p_e) {
            exc = true;
        }

        System.out.println("RESIZE");
        expect = "*********************************************************************************************HashtableChunkSize:130Bytes***HEADER***Depth:4***DATA***0xAAAA<==2or00xAAAA<==10or10xAAAA<==18or20xAAAA<==26or30xBBBB<==34or40xBBBB<==42or50xBBBB<==50or60xBBBB<==58or70xCCCC<==66or80xCCCC<==74or90xCCCC<==82or100xCCCC<==90or110xDDDD<==98or120xDDDD<==106or130xDDDD<==114or140xDDDD<==122or15*********************************************************************************************";
        try {
            structuredTest.resize(expect, 4);
        } catch (RuntimeException p_e) {
            exc = true;
        }

        System.out.println("LOOKUP");
        structuredTest.lookup(Skema.serialize(0xAAAA), 0);
        structuredTest.lookup(Skema.serialize(0xAAAA), 3);
        structuredTest.lookup(Skema.serialize(0xBBBB), 4);
        structuredTest.lookup(Skema.serialize(0xBBBB), 7);
        structuredTest.lookup(Skema.serialize(0xCCCC), 8);
        structuredTest.lookup(Skema.serialize(0xCCCC), 11);
        structuredTest.lookup(Skema.serialize(0xDDDD), 12);
        structuredTest.lookup(Skema.serialize(0xDDDD), 15);

        if (!exc)
            structuredTest.end();
    }
}

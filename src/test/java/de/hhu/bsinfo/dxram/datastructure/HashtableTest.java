package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.skema.Skema;
import org.junit.Test;

public class HashtableTest {

    @Test
    public void runStructuredTest() {
        DXMem memory = new DXMem((short) 0xAACD, 10000000);
        int initialSize = (int) Math.pow(2, 2) * Long.BYTES + 2;
        long cid = memory.create().create(initialSize);

        HashtableStructuredTest structuredTest = new HashtableStructuredTest(memory, cid);

        System.out.println("INIT");
        structuredTest.init(0xAAAA, initialSize);

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

        System.out.println("RESIZE");
        expect = "*********************************************************************************************HashtableChunkSize:66Bytes***HEADER***Depth:3***DATA***0xAAAA<==20xAAAA<==100xBBBB<==180xBBBB<==260xCCCC<==340xCCCC<==420xDDDD<==500xDDDD<==58*********************************************************************************************";
        structuredTest.resize(expect, 3);

        System.out.println("RESIZE");
        expect = "*********************************************************************************************HashtableChunkSize:130Bytes***HEADER***Depth:4***DATA***0xAAAA<==20xAAAA<==100xAAAA<==180xAAAA<==260xBBBB<==340xBBBB<==420xBBBB<==500xBBBB<==580xCCCC<==660xCCCC<==740xCCCC<==820xCCCC<==900xDDDD<==980xDDDD<==1060xDDDD<==1140xDDDD<==122*********************************************************************************************";
        structuredTest.resize(expect, 4);

        System.out.println("LOOKUP");
        structuredTest.lookup(Skema.serialize(0xAAAA), 0);
        structuredTest.lookup(Skema.serialize(0xAAAA), 3);
        structuredTest.lookup(Skema.serialize(0xBBBB), 4);
        structuredTest.lookup(Skema.serialize(0xBBBB), 7);
        structuredTest.lookup(Skema.serialize(0xCCCC), 8);
        structuredTest.lookup(Skema.serialize(0xCCCC), 11);
        structuredTest.lookup(Skema.serialize(0xDDDD), 12);
        structuredTest.lookup(Skema.serialize(0xDDDD), 15);

        structuredTest.end();
    }
}

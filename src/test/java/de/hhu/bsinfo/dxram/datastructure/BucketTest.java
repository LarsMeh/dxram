package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import org.junit.Test;

/**
 * This will Test the Memory Layout Bucket with primitive and NonPrimitive methods from the StructuredTest.
 * But care. One Test is only Visual. So if you have a problem while using the Bucket.class take a look, you will find
 * three outputs from Bucket (before,after the split) and the n-Bit compare output with bitmask.
 */
public class BucketTest {

    @Test
    public void runStructuredTest() {
        DXMem memory = new DXMem((short) 0xAACD, 10000000);
        long cid = memory.create().create(200);
        long cid_2 = memory.create().create(200);

        BucketStructuredTest structuredTest = new BucketStructuredTest(memory, cid, cid_2);

        System.out.println("INIT");
        structuredTest.init();

        System.out.println("PUT");
        structuredTest.put();

        System.out.println("CONTAINS");
        structuredTest.contains();

        System.out.println("GET");
        structuredTest.get();

        System.out.println("REMOVE");
        structuredTest.remove();

        System.out.println("PUT");
        structuredTest.put();

        System.out.println("REMOVE");
        structuredTest.remove2();

        System.out.println("PUT");
        structuredTest.put();

        System.out.println("SIZE FOR FIT");
        structuredTest.sizeForFit(200, 0);
        structuredTest.sizeForFit(200, 144);
        structuredTest.sizeForFit(201, 145);
        structuredTest.sizeForFit(200, -100);

        System.out.println("BUCKET SPLIT");
        structuredTest.splitBucketVisualize();

        structuredTest.end();
    }

    @Test
    public void runStructuredTestNonPrimitive() {
        DXMem memory = new DXMem((short) 0xAACD, 10000000);
        long cid = memory.create().create(200);
        long cid_2 = memory.create().create(200);

        BucketStructuredTest structuredTest = new BucketStructuredTest(memory, cid, cid_2);

        System.out.println("INIT");
        structuredTest.init();

        System.out.println("PUT");
        structuredTest.putNonPrimitive();

        System.out.println("CONTAINS");
        structuredTest.containsNonPrimitive();

        System.out.println("GET");
        structuredTest.getNonPrimitive();

        System.out.println("REMOVE");
        structuredTest.removeNonPrimitive();

        System.out.println("PUT");
        structuredTest.putNonPrimitive();

        System.out.println("SIZE FOR FIT");
        structuredTest.sizeForFit(200, 0);
        structuredTest.sizeForFit(200, 16);
        structuredTest.sizeForFit(201, 17);
        structuredTest.sizeForFit(200, -100);

        System.out.println("BUCKET SPLIT");
        structuredTest.splitBucketVisualize();

        structuredTest.end();
    }

    @Test
    public void runStructuredTestNonPrimitive2() {
        DXMem memory = new DXMem((short) 0xAACD, 10000000);
        long cid = memory.create().create(200);
        long cid_2 = memory.create().create(200);

        BucketStructuredTest structuredTest = new BucketStructuredTest(memory, cid, cid_2);

        System.out.println("INIT");
        structuredTest.init();

        System.out.println("PUT");
        structuredTest.putNonPrimitive();

        System.out.println("CONTAINS");
        structuredTest.containsNonPrimitive();

        System.out.println("GET");
        structuredTest.getNonPrimitive();

        System.out.println("REMOVE");
        structuredTest.removeNonPrimitive();

        System.out.println("VISUALIZE");
        structuredTest.visualize();

        System.out.println("PUT");
        structuredTest.putNonPrimitive();

        System.out.println("SIZE FOR FIT");
        structuredTest.sizeForFit(200, 0);
        structuredTest.sizeForFit(200, 16);
        structuredTest.sizeForFit(201, 17);
        structuredTest.sizeForFit(200, -100);

        System.out.println("BUCKET SPLIT");
        structuredTest.splitBucketVisualize2();

        structuredTest.end();
    }

}
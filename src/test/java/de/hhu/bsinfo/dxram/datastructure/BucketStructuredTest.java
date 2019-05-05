package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.skema.Skema;
import org.junit.Assert;

import java.util.Arrays;

public class BucketStructuredTest extends StructuredTest {

    private long m_cid_2;
    private long m_address_2;

    public BucketStructuredTest(DXMem p_memory, final long p_cid, final long p_cid_2) {
        super(p_memory, p_cid);
        m_cid_2 = p_cid_2;
    }

    @Override
    protected void pin() {
        super.pin();
        m_address = m_memory.pinning().pin(m_cid_2).getAddress();
    }

    @Override
    protected void unpin() {
        super.unpin();
        m_memory.pinning().unpinCID(m_cid_2);
    }

    public void end() {
        unpin();
    }

    public void init() {

        pin();

        // Test
        Bucket.initialize(m_writer, m_address, (short) 0);

        // Check header
        Assert.assertEquals(false, Bucket.isFull(m_reader, m_address));
        Assert.assertEquals((short) 0, Bucket.getDepth(m_reader, m_address));
        Assert.assertEquals(0, Bucket.getUsedBytes(m_reader, m_address, true));
        Assert.assertEquals((short) 0, Bucket.getSize(m_reader, m_address));
    }

    public void put() {

        // Test
        for (int i = 0; i < HashMap.BUCKET_ENTRIES; i++) {
            //System.out.println("Put: " + i);
            Bucket.put(m_reader, m_writer, m_address, Skema.serialize(i + 1), Skema.serialize(100 + i + 1));
        }

        //System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        // Check header
        Assert.assertEquals(true, Bucket.isFull(m_reader, m_address));
        Assert.assertEquals((short) 0, Bucket.getDepth(m_reader, m_address));
        Assert.assertEquals(HashMap.BUCKET_ENTRIES * 2 * (Integer.BYTES + Short.BYTES), Bucket.getUsedBytes(m_reader, m_address, true));
        Assert.assertEquals((short) HashMap.BUCKET_ENTRIES, Bucket.getSize(m_reader, m_address));
        Assert.assertEquals(false, Bucket.isEnoughSpace(m_reader, m_size, m_cid, m_address, 145));
        Assert.assertEquals(true, Bucket.isEnoughSpace(m_reader, m_size, m_cid, m_address, 144));
    }

    public void remove() {

        // Test
        for (int i = 0; i < HashMap.BUCKET_ENTRIES; i++) {
            Bucket.remove(m_reader, m_writer, m_address, Skema.serialize(i + 1), Skema.serialize(100 + i + 1));
        }

        //System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        // Check header
        Assert.assertEquals(false, Bucket.isFull(m_reader, m_address));
        Assert.assertEquals((short) 0, Bucket.getDepth(m_reader, m_address));
        Assert.assertEquals(0, Bucket.getUsedBytes(m_reader, m_address, true));
        Assert.assertEquals((short) 0, Bucket.getSize(m_reader, m_address));
        Assert.assertEquals(true, Bucket.isEnoughSpace(m_reader, m_size, m_cid, m_address, 145));

    }

    public void remove2() {

        // Test
        for (int i = 0; i < HashMap.BUCKET_ENTRIES; i++) {
            byte[] value = Skema.serialize(100 + i + 1);
            byte[] retValue = Bucket.remove(m_reader, m_writer, m_address, Skema.serialize(i + 1));
            Assert.assertArrayEquals(value, retValue);
        }

        //System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        // Check header
        Assert.assertEquals(false, Bucket.isFull(m_reader, m_address));
        Assert.assertEquals((short) 0, Bucket.getDepth(m_reader, m_address));
        Assert.assertEquals(0, Bucket.getUsedBytes(m_reader, m_address, true));
        Assert.assertEquals((short) 0, Bucket.getSize(m_reader, m_address));
        Assert.assertEquals(true, Bucket.isEnoughSpace(m_reader, m_size, m_cid, m_address, 145));

    }

    public void get() {

        // Test
        for (int i = 0; i < HashMap.BUCKET_ENTRIES; i++) {
            byte[] key = Skema.serialize(i + 1);
            byte[] value = Skema.serialize(100 + i + 1);
            byte[] result = Bucket.get(m_reader, m_address, key);
            Assert.assertArrayEquals(value, result);
        }

        //System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        // Check header
        Assert.assertEquals(true, Bucket.isFull(m_reader, m_address));
        Assert.assertEquals((short) 0, Bucket.getDepth(m_reader, m_address));
        Assert.assertEquals(HashMap.BUCKET_ENTRIES * 2 * (Integer.BYTES + Short.BYTES), Bucket.getUsedBytes(m_reader, m_address, true));
        Assert.assertEquals((short) HashMap.BUCKET_ENTRIES, Bucket.getSize(m_reader, m_address));
    }

    public void splitBucketVisualize() {

        System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        Bucket.splitBucket(m_reader, m_writer, m_address, m_address_2, (byte) 1);

        // Visualize
        System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));
        System.out.println(Bucket.toString(m_size, m_reader, m_cid_2, m_address_2));
    }

    public void sizeForFit(final int p_expect, final int p_input) {

        // Test
        Assert.assertEquals(p_expect, Bucket.sizeForFit(m_reader, m_size, m_cid, m_address, p_input));
    }

    public void putNonPrimitive() {

        byte[] firstKey = Skema.serialize("ABC");
        byte[] firstValue = Skema.serialize("VALUE ZU abc");

        Bucket.put(m_reader, m_writer, m_address, firstKey, firstValue);

        Assert.assertEquals(Bucket.calcStoredSize(firstKey, firstValue), Bucket.getUsedBytes(m_reader, m_address, true));

        Bucket.put(m_reader, m_writer, m_address, Skema.serialize("DEFG"), Skema.serialize("VALUE ZU defg"));
        Bucket.put(m_reader, m_writer, m_address, Skema.serialize("HIJKL"), Skema.serialize("VALUE ZU Hijkl"));
        Bucket.put(m_reader, m_writer, m_address, Skema.serialize("MN"), Skema.serialize("VALUE ZU mN"));
    }

    public void getNonPrimitive() {

        Assert.assertArrayEquals(Skema.serialize("VALUE ZU abc"), Bucket.get(m_reader, m_address, Skema.serialize("ABC")));
        Assert.assertArrayEquals(Skema.serialize("VALUE ZU defg"), Bucket.get(m_reader, m_address, Skema.serialize("DEFG")));
        Assert.assertArrayEquals(Skema.serialize("VALUE ZU Hijkl"), Bucket.get(m_reader, m_address, Skema.serialize("HIJKL")));
        Assert.assertArrayEquals(Skema.serialize("VALUE ZU mN"), Bucket.get(m_reader, m_address, Skema.serialize("MN")));
    }

    public void removeNonPrimitive() {
        byte[] ret;
        ret = Bucket.remove(m_reader, m_writer, m_address, Skema.serialize("ABC"));
        Assert.assertArrayEquals(Skema.serialize("VALUE ZU abc"), ret);

        ret = Bucket.remove(m_reader, m_writer, m_address, Skema.serialize("DEFG"));
        Assert.assertArrayEquals(Skema.serialize("VALUE ZU defg"), ret);

        boolean res;
        res = Bucket.remove(m_reader, m_writer, m_address, Skema.serialize("HIJKL"), Skema.serialize("VALUE ZU Hijkl"));
        Assert.assertEquals(true, res);
        res = Bucket.remove(m_reader, m_writer, m_address, Skema.serialize("MN"), Skema.serialize("VALUE ZU mN"));
        Assert.assertEquals(true, res);
    }

    public void contains() {
        boolean result;

        result = Bucket.contains(m_reader, m_address, Skema.serialize(1));
        Assert.assertEquals(true, result);

        result = Bucket.contains(m_reader, m_address, Skema.serialize(0));
        Assert.assertEquals(false, result);

    }

    public void containsNonPrimitive() {
        boolean result;

        result = Bucket.contains(m_reader, m_address, Skema.serialize("ABC"));
        Assert.assertEquals(true, result);

        result = Bucket.contains(m_reader, m_address, Skema.serialize("ZAH"));
        Assert.assertEquals(false, result);

    }

    public void splitBucketVisualize2() {

        System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));

        //Bucket.splitBucket(m_reader, m_writer, m_address, m_address_2, (byte) 1);
        Bucket.RawData raw = Bucket.splitBucket(m_reader, m_writer, m_address, (byte) 1);

        // Visualize
        System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));
        byte[] arr = raw.getByteArray();
        System.out.println("Raw as Byte Array" + Arrays.toString(arr) + "\nWith length = " + arr.length);
        System.out.println("Data bytes = " + raw.getDataBytes());
    }

    public void visualize(){

        System.out.println(Bucket.toString(m_size, m_reader, m_cid, m_address));
    }

}

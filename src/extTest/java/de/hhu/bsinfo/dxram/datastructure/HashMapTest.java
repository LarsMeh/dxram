package de.hhu.bsinfo.dxram.datastructure;


import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import de.hhu.bsinfo.dxram.datastructure.util.Stopwatch;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HashMapTest {

    private static final String m_id = "A";

    private static final int m_capac = 2;

    private static final int m_nodes = 1;

    @TestInstance(runOnNodeIdx = 1)
    public void createHashMapLocal(final DXRAM p_instance) {
        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

    }

    @TestInstance(runOnNodeIdx = 1)
    public void simplePutLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        map.put("ABCD", "EFGHIJ"); // String

        HashMap<Integer, Integer> map2 = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        map2.put(4, 6); // Integer

        HashMap<byte[], byte[]> map3 = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        byte[] key = {1, 2, 3};
        byte[] value = {4, 5, 6};
        map3.put(key, value); // Byte Array
        Assert.assertArrayEquals(value, map3.remove(key));

    }

    @TestInstance(runOnNodeIdx = 1)
    public void simplePutGetRemoveLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<Integer, Integer> map = service.createHashMap(m_id, m_capac, m_nodes, 4, 4, (byte) 1, false);

        for (int i = 0; i < 6; i++) {
            map.put(i, i + 1);
        }

        Assert.assertEquals(6, map.size());

        for (int i = 0; i < 6; i++) {
            Assert.assertEquals(true, (i + 1) == map.get(i));
        }

        Assert.assertEquals(6, map.size());

        for (int i = 0; i < 6; i++) {
            Assert.assertEquals(true, (i + 1) == map.remove(i));
        }

        Assert.assertEquals(0, map.size());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void complexPutAndRemoveLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        //map.put("ABCD", "EFGHIJ");
        map.put("ABCD", "CDEFH");
        //map.put("BCD", "CDEFH");
        map.put("AD", "CDEFH");
        map.put("ACD", "CDEFH");
        map.put("BCD", "CDEFH");
        map.put("A", "CDEFH");
        map.put("D", "CDEFH");
        map.put("BD", "CDEFH");
        map.put("EGFHD", "ASDFH");
        map.put("FHD", "ASDFH");
        map.put("EFHD", "ASDFH");
        map.put("EFD", "ASDFH");

        Assert.assertEquals(11, map.size());

        Assert.assertEquals("CDEFH", map.remove("ABCD"));
        Assert.assertEquals("CDEFH", map.remove("ACD"));
        Assert.assertEquals("CDEFH", map.remove("BD"));
        Assert.assertEquals("ASDFH", map.remove("EFHD"));

        Assert.assertEquals(7, map.size());

    }

    @TestInstance(runOnNodeIdx = 1)
    public void complexPutAndGetLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        map.put("ABCD", "EFGHIJ");
        map.put("ABCD", "CDEFH");
        map.put("BCD", "CDEFH");
        map.put("AD", "CDEFH");
        map.put("ACD", "CDEFH");
        map.put("ABCD", "CDEFH");
        map.put("BCD", "CDEFH");
        map.put("A", "CDEFH");
        map.put("D", "CDEFH");
        map.put("BD", "CDEFH");
        map.put("EGFHD", "ASDFH");
        map.put("FHD", "ASDFH");
        map.put("EFHD", "ASDFH");
        map.put("EFD", "ASDFH");

        Assert.assertEquals(11, map.size());

        Assert.assertEquals("CDEFH", map.get("ABCD"));
        Assert.assertEquals("CDEFH", map.get("ACD"));
        Assert.assertEquals("CDEFH", map.get("BD"));
        Assert.assertEquals("ASDFH", map.get("EFHD"));

        Assert.assertEquals(11, map.size());

    }

    @TestInstance(runOnNodeIdx = 1)
    public void clearLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);

        map.put("ABCD", "EFGHIJ");
        map.put("ABCD", "CDEFH");
        map.put("BCD", "CDEFH");
        map.put("AD", "CDEFH");
        map.put("ACD", "CDEFH");
        map.put("ABCD", "CDEFH");
        map.put("BCD", "CDEFH");
        map.put("A", "CDEFH");
        map.put("D", "CDEFH");
        map.put("BD", "CDEFH");
        map.put("EGFHD", "ASDFH");
        map.put("FHD", "ASDFH");
        map.put("EFHD", "ASDFH");
        map.put("EFD", "ASDFH");

        Assert.assertEquals(11, map.size());

        map.clear();

        Assert.assertEquals(0, map.size());

    }

    @TestInstance(runOnNodeIdx = 1)
    public void extremePut(final DXRAM p_instance) {
        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<Integer, Integer> map = service.createHashMap(m_id, 5000000, m_nodes, 4, 4, HashFunctions.MURMUR3_32, true);

        for (int i = 0; i < 5000; i++) {
            map.put(i, i + 1);
        }

    }
}

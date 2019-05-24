package de.hhu.bsinfo.dxram.datastructure;


import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.Assert;
import org.junit.runner.RunWith;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HashMapGlobalTest {

    private static final String m_id = "B";

    private static final int m_capac = 2;

    private static final int m_nodes = 2;

//    @TestInstance(runOnNodeIdx = 2)
//    public void complexPutAndRemoveGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);
//
//        Assert.assertEquals(0, map.size());
//
//        map.put("ABCD", "EFGHIJ");
//        map.put("ABCD", "CDEFH"); // overwrite
//        map.put("ABCDE", "CDEFH");
//        map.put("BCD", "CDEFH");
//        map.put("AD", "CDEFH");
//        map.put("ACD", "CDEFH");
//        map.put("A", "CDEFH");
//        map.put("D", "CDEFH");
//        map.put("BD", "CDEFH");
//        map.put("EGFZ", "ASDFH");
//        map.put("FHD", "ASDFH");
//        map.put("EFHD", "ASDFH");
//        map.put("EFD", "ASDFH");
//
//        Assert.assertEquals(12, map.size());
//
//
//        Assert.assertEquals("CDEFH", map.remove("ABCD"));
//        Assert.assertEquals("CDEFH", map.remove("ACD"));
//        Assert.assertEquals("CDEFH", map.remove("BD"));
//        Assert.assertEquals("ASDFH", map.remove("EFHD"));
//
//        Assert.assertEquals(8, map.size());
//
//    }
//
//    @TestInstance(runOnNodeIdx = 2)
//    public void complexPutAndGetGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);
//
//        map.put("ABCD", "EFGHIJ");
//        map.put("ABCD", "CDEFH"); // overwrite
//        map.put("ABCDE", "CDEFH");
//        map.put("BCD", "CDEFH");
//        map.put("AD", "CDEFH");
//        map.put("ACD", "CDEFH");
//        map.put("A", "CDEFH");
//        map.put("D", "CDEFH");
//        map.put("BD", "CDEFH");
//        map.put("EGFZ", "ASDFH");
//        map.put("FHD", "ASDFH");
//        map.put("EFHD", "ASDFH");
//        map.put("EFD", "ASDFH");
//
//        Assert.assertEquals(12, map.size());
//
//
//        Assert.assertEquals("CDEFH", map.get("ABCD"));
//        Assert.assertEquals("CDEFH", map.get("ACD"));
//        Assert.assertEquals("CDEFH", map.get("BD"));
//        Assert.assertEquals("ASDFH", map.get("EFHD"));
//
//        Assert.assertEquals(12, map.size());
//
//    }
//
//
//    @TestInstance(runOnNodeIdx = 2)
//    public void clearGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1, false);
//
//        map.put("ABCD", "EFGHIJ");
//        map.put("ABCD", "CDEFH");
//        map.put("BCD", "CDEFH");
//        map.put("AD", "CDEFH");
//        map.put("ACD", "CDEFH");
//        map.put("ABCD", "CDEFH");
//        map.put("A", "CDEFH");
//        map.put("D", "CDEFH");
//        map.put("BD", "CDEFH");
//        map.put("EGFHD", "ASDFH");
//        map.put("FHD", "ASDFH");
//        map.put("EFHD", "ASDFH");
//        map.put("EFD", "ASDFH");
//
//        Assert.assertEquals(11, map.size());
//
//        Assert.assertEquals("CDEFH", map.get("ABCD"));
//        Assert.assertEquals("CDEFH", map.get("ACD"));
//        Assert.assertEquals("CDEFH", map.get("BD"));
//        Assert.assertEquals("ASDFH", map.get("EFHD"));
//
//        Assert.assertEquals(11, map.size());
//
//        map.clear();
//
//        Assert.assertEquals(0, map.size());
//
//    }

    @TestInstance(runOnNodeIdx = 1)
    public void extremePut(final DXRAM p_instance) {
        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<Integer, Integer> map = service.createHashMap(m_id, 2, m_nodes, 4, 4, HashFunctions.MURMUR3_32, true);

        // put : 1843 (exp = 4)

        // Put
        map.put(0x20000000, 1);
        map.put(0x80000000, 2);
        map.logHashtable();
        map.put(0x10000000, 3);
        map.put(0xA0000000, 4);
        map.logHashtable();
        map.put(0x40000000, 5);
        map.put(0xC0000000, 6);
        map.logHashtable();
        map.put(0x60000000, 7);
        map.put(0xE0000000, 8);
        map.put(0xF0000000, 9);
        map.logHashtable();

        // Get
        map.get(0x20000000);
        map.get(0x80000000);
        map.get(0x10000000);
        map.get(0xA0000000);
        map.get(0x40000000);
        map.get(0xC0000000);
        map.get(0x60000000);
        map.get(0xE0000000);
    }
}
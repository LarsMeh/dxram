package de.hhu.bsinfo.dxram.datastructure;


import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.Assert;
import org.junit.runner.RunWith;

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

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);


    }

    @TestInstance(runOnNodeIdx = 1)
    public void simplePutLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);

        map.put("ABCD", "EFGHIJ");

    }

    @TestInstance(runOnNodeIdx = 1)
    public void complexPutAndRemoveLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);

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

        Assert.assertEquals(14, map.size());

        Assert.assertEquals("CDEFH", map.remove("ABCD"));
        Assert.assertEquals("CDEFH", map.remove("ACD"));
        Assert.assertEquals("CDEFH", map.remove("BD"));
        Assert.assertEquals("ASDFH", map.remove("EFHD"));

        Assert.assertEquals(10, map.size());

    }

    @TestInstance(runOnNodeIdx = 1)
    public void complexPutAndGetLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);

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

        Assert.assertEquals(14, map.size());

        Assert.assertEquals("CDEFH", map.get("ABCD"));
        Assert.assertEquals("CDEFH", map.get("ACD"));
        Assert.assertEquals("CDEFH", map.get("BD"));
        Assert.assertEquals("ASDFH", map.get("EFHD"));

        Assert.assertEquals(14, map.size());

    }

    @TestInstance(runOnNodeIdx = 1)
    public void clearLocal(final DXRAM p_instance) {

        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);

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

        Assert.assertEquals(14, map.size());

        Assert.assertEquals("CDEFH", map.get("ABCD"));
        Assert.assertEquals("CDEFH", map.get("ACD"));
        Assert.assertEquals("CDEFH", map.get("BD"));
        Assert.assertEquals("ASDFH", map.get("EFHD"));

        Assert.assertEquals(14, map.size());

        map.clear();

        Assert.assertEquals(0, map.size());

    }
}

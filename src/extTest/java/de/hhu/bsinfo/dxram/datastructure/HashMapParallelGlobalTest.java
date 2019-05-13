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
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class HashMapParallelGlobalTest {

    private static final String m_id = "B";

    private static final int m_capac = 2;

    private static final int m_nodes = 8;

//    @TestInstance(runOnNodeIdx = 2)
//    public void complexPutAndRemoveGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);
//
//        Assert.assertEquals(0, map.size());
//
//        Thread t1 = new Thread(() -> {
//            System.out.println("###T1 FIRST###");
//            map.put("ABCD", "EFGHIJ");
//            System.out.println("###T1###");
//            map.put("ABCD", "CDEFH"); // overwrite
//            System.out.println("###T1###");
//            map.put("ABCDE", "CDEFH");
//
//            map.put("ZXY", "CDEFH");
//            map.put("xyV", "CDEFH");
//            map.put("ietner", "CDEFH");
//            map.put("lkgfmbng", "CDEFH");
//            map.put("agdfibe", "CDEFH");
//            map.put("difjoaisdhf", "CDEFH");
//            map.put("217bdwba", "CDEFH");
//            map.put("234jnonfwd", "CDEFH");
//            map.put("Adqf2d2edsadsf", "CDEFH");
//
//            System.out.println("###T1###");
//            map.put("BCD", "CDEFH");
//            System.out.println("###T1###");
//            map.put("AD", "CDEFH");
//            System.out.println("###T1 LAST###");
//            map.put("ACD", "CDEFH");
//
//        });
//
//        Thread t2 = new Thread(() -> {
//            System.out.println("***T2 FIRST***");
//            map.put("A", "CDEFH");
//            System.out.println("***T2***");
//            map.put("D", "CDEFH");
//            System.out.println("***T2***");
//            map.put("BD", "CDEFH");
//            System.out.println("***T2***");
//            map.put("EGFZ", "ASDFH");
//            System.out.println("***T2***");
//            map.put("FHD", "ASDFH");
//            System.out.println("***T2***");
//            map.put("EFHD", "ASDFH");
//            System.out.println("***T2 LAST***");
//            map.put("EFD", "ASDFH");
//        });
//
//        try {
//            t1.start();
//            t2.start();
//            t1.join();
//            t2.join();
//        } catch (InterruptedException p_e) {
//            p_e.printStackTrace();
//        }
//
//
//        Assert.assertEquals(21, map.size());
//
//
//        Assert.assertEquals("CDEFH", map.remove("ABCD"));
//        Assert.assertEquals("CDEFH", map.remove("ACD"));
//        Assert.assertEquals("CDEFH", map.remove("BD"));
//        Assert.assertEquals("ASDFH", map.remove("EFHD"));
//
//        Assert.assertEquals(17, map.size());
//
//    }
//
//    @TestInstance(runOnNodeIdx = 2)
//    public void complexPutAndGetGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);
//
//        Thread t1 = new Thread(() -> {
//            System.out.println("###T1 FIRST###");
//            map.put("ABCD", "EFGHIJ");
//            System.out.println("###T1###");
//            map.put("ABCD", "CDEFH"); // overwrite
//            System.out.println("###T1###");
//            map.put("ABCDE", "CDEFH");
//
//            map.put("ZXY", "CDEFH");
//            map.put("xyV", "CDEFH");
//            map.put("ietner", "CDEFH");
//            map.put("lkgfmbng", "CDEFH");
//            map.put("agdfibe", "CDEFH");
//            map.put("difjoaisdhf", "CDEFH");
//            map.put("217bdwba", "CDEFH");
//            map.put("234jnonfwd", "CDEFH");
//            map.put("Adqf2d2edsadsf", "CDEFH");
//
//            System.out.println("###T1###");
//            map.put("BCD", "CDEFH");
//            System.out.println("###T1###");
//            map.put("AD", "CDEFH");
//            System.out.println("###T1 LAST###");
//            map.put("ACD", "CDEFH");
//
//        });
//
//        Thread t2 = new Thread(() -> {
//            System.out.println("***T2 FIRST***");
//            map.put("A", "CDEFH");
//            System.out.println("***T2***");
//            map.put("D", "CDEFH");
//            System.out.println("***T2***");
//            map.put("BD", "CDEFH");
//            System.out.println("***T2***");
//            map.put("EGFZ", "ASDFH");
//            System.out.println("***T2***");
//            map.put("FHD", "ASDFH");
//            System.out.println("***T2***");
//            map.put("EFHD", "ASDFH");
//            System.out.println("***T2 LAST***");
//            map.put("EFD", "ASDFH");
//        });
//
//        try {
//            t1.start();
//            t2.start();
//            t1.join();
//            t2.join();
//        } catch (InterruptedException p_e) {
//            p_e.printStackTrace();
//        }
//
//        Assert.assertEquals(21, map.size());
//
//
//        Assert.assertEquals("CDEFH", map.get("ABCD"));
//        Assert.assertEquals("CDEFH", map.get("ACD"));
//        Assert.assertEquals("CDEFH", map.get("BD"));
//        Assert.assertEquals("ASDFH", map.get("EFHD"));
//
//        Assert.assertEquals(21, map.size());
//
//    }
//
//
//    @TestInstance(runOnNodeIdx = 2)
//    public void clearGlobal(final DXRAM p_instance) {
//
//        DataStructureService service = p_instance.getService(DataStructureService.class);
//
//        HashMap<String, String> map = service.createHashMap(m_id, m_capac, m_nodes, 1, 1, (byte) 1);
//
//        Thread t1 = new Thread(() -> {
//            System.out.println("###T1 FIRST###");
//            map.put("ABCD", "EFGHIJ");
//            System.out.println("###T1###");
//            map.put("ABCD", "CDEFH"); // overwrite
//            System.out.println("###T1###");
//            map.put("ABCDE", "CDEFH");
//
//            map.put("ZXY", "CDEFH");
//            map.put("xyV", "CDEFH");
//            map.put("ietner", "CDEFH");
//            map.put("lkgfmbng", "CDEFH");
//            map.put("agdfibe", "CDEFH");
//            map.put("difjoaisdhf", "CDEFH");
//            map.put("217bdwba", "CDEFH");
//            map.put("234jnonfwd", "CDEFH");
//            map.put("Adqf2d2edsadsf", "CDEFH");
//
//            System.out.println("###T1###");
//            map.put("BCD", "CDEFH");
//            System.out.println("###T1###");
//            map.put("AD", "CDEFH");
//            System.out.println("###T1 LAST###");
//            map.put("ACD", "CDEFH");
//
//        });
//
//        Thread t2 = new Thread(() -> {
//            System.out.println("***T2 FIRST***");
//            map.put("A", "CDEFH");
//            System.out.println("***T2***");
//            map.put("D", "CDEFH");
//            System.out.println("***T2***");
//            map.put("BD", "CDEFH");
//            System.out.println("***T2***");
//            map.put("EGFZ", "ASDFH");
//            System.out.println("***T2***");
//            map.put("FHD", "ASDFH");
//            System.out.println("***T2***");
//            map.put("EFHD", "ASDFH");
//            System.out.println("***T2 LAST***");
//            map.put("EFD", "ASDFH");
//        });
//
//        try {
//            t1.start();
//            t2.start();
//            t1.join();
//            t2.join();
//        } catch (InterruptedException p_e) {
//            p_e.printStackTrace();
//        }
//
//        map.clear();
//
//        Assert.assertEquals(0, map.size());
//
//    }

    @TestInstance(runOnNodeIdx = 1)
    public void extremePut(final DXRAM p_instance) {
        DataStructureService service = p_instance.getService(DataStructureService.class);

        HashMap<Integer, Integer> map = service.createHashMap(m_id, 5000, m_nodes, 4, 4, HashFunctions.MURMUR3_32);

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 2500; i++) {
                map.put(i, i + 1);
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 2500; i < 5000; i++) {
                map.put(i, i + 1);
            }
        });

        try {
            t1.start();
            t2.start();
            t1.join();
            t2.join();
        } catch (InterruptedException p_e) {
            p_e.printStackTrace();
        }

        map.clear();

        Assert.assertEquals(0, map.size());

    }
}

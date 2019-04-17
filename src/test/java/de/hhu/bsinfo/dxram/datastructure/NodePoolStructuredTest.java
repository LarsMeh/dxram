package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;

import java.util.ArrayList;

public class NodePoolStructuredTest extends StructuredTest {

    private final ArrayList<Short> m_onlinePeers;

    public NodePoolStructuredTest(DXMem p_memory, final ArrayList<Short> p_onlinePeers) {
        super(p_memory,0L);
        m_onlinePeers = p_onlinePeers;
    }

    public void init(final int p_numberOfPeers) {

        // Test
        int initialSize = NodePool.getInitialMemorySize(m_onlinePeers.size(), p_numberOfPeers);
        m_cid = m_memory.create().create(initialSize);
        pin();

        NodePool.initialize(m_writer, m_onlinePeers, m_address, p_numberOfPeers);

        //System.out.println(NodePool.toString(m_size, m_reader, m_cid, m_address));
    }

    public void randomAccessVisualize(final int p_iterations) {

        // Test
        for (int i = 0; i < p_iterations; i++) {
            System.out.println("0x" + NodePool.getRandomNode(m_reader, m_address));
        }
    }

    public void end() {
        unpin();
    }

}

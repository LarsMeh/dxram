package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxmem.operations.Size;

public abstract class StructuredTest {

    final DXMem m_memory;
    final RawRead m_reader;
    final RawWrite m_writer;
    final Size m_size;
    long m_cid;
    long m_address;

    public StructuredTest(DXMem p_memory, long p_cid) {
        m_memory = p_memory;
        m_reader = m_memory.rawRead();
        m_writer = m_memory.rawWrite();
        m_size = m_memory.size();
        m_cid = p_cid;
    }

    protected void pin() {
        m_address = m_memory.pinning().pin(m_cid).getAddress();
    }

    protected void unpin() {
        m_memory.pinning().unpinCID(m_cid);
    }

    public abstract void end();
}

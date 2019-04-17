package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import org.junit.Assert;

public class HashtableStructuredTest extends StructuredTest {

    public HashtableStructuredTest(DXMem p_memory, final long p_cid) {
        super(p_memory,p_cid);
    }

    @Override
    public void end() {
        unpin();
    }

    public void init(final long p_value, final int p_size) {

        pin();

        // Test
        Hashtable.initialize(m_writer, m_address, p_size, 2, p_value);

        //System.out.println(Hashtable.toString(m_size, m_reader, m_cid, m_address));

        Assert.assertEquals(2, Hashtable.getDepth(m_reader, m_address));
    }

    public void split(final long p_suspect, final long p_value, final int p_position) {

        // Test
        Hashtable.splitForEntry(m_reader, m_writer, m_size, m_cid, m_address, p_suspect, p_position, p_value);

        //System.out.println(Hashtable.toString(m_size, m_reader, m_cid, m_address));
    }

    public void resize(String p_expectedOutput, final int p_expected) {

        // Test
        long retAddress = Hashtable.resize(m_reader, m_writer, m_memory.resize(), m_memory.pinning(), m_cid, m_address);

        if (retAddress != m_address)
            m_address = retAddress;

        p_expectedOutput = p_expectedOutput.replace(" ", "");
        String result = Hashtable.toString(m_size, m_reader, m_cid, m_address).replace(" ", "").replace("\n", "");
        Assert.assertEquals(p_expectedOutput, result);
        Assert.assertEquals(p_expected, Hashtable.getDepth(m_reader, m_address));

    }

    public void lookup(final byte[] p_expect, final int p_input) {

        // Test
        Hashtable.lookup(m_reader, m_address, p_input);
    }

}

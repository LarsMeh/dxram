package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import org.junit.Assert;

public class HashMapMetadataStructuredTest extends StructuredTest {

    public HashMapMetadataStructuredTest(DXMem p_memory) {
        super(p_memory, 0L);
    }

    public void init(final long p_table, final long p_nodepool, final long p_size, final short p_skemaId,
                     final int p_individual_bucket_size, final byte p_hashFunctionId) {

        m_cid = m_memory.create().create(Metadata.getInitialMemorySize());

        pin();

        // Test
        Metadata.initialize(m_writer, m_address, p_table, p_nodepool, p_size, p_skemaId, p_individual_bucket_size, p_hashFunctionId);

        Assert.assertEquals(p_table, Metadata.getHashtable(m_reader, m_address));
        Assert.assertEquals(p_nodepool, Metadata.getNodepool(m_reader, m_address));
        Assert.assertEquals(p_size, Metadata.getHashMapSize(m_reader, m_address));
        Assert.assertEquals(p_skemaId, Metadata.getSkemaValueId(m_reader, m_address));
        Assert.assertEquals(p_individual_bucket_size, Metadata.getIndividualBucketSize(m_reader, m_address));
        Assert.assertEquals(p_hashFunctionId, Metadata.getHashFunctionId(m_reader, m_address));
    }

    public void incrementSize(final int p_expect, final int p_iterations) {

        // Test
        for (int i = 0; i < p_iterations; i++) {
            Metadata.incrementSize(m_writer, m_reader, m_address);
        }

        Assert.assertEquals(p_expect, Metadata.getHashMapSize(m_reader, m_address));
    }

    public void decrementSize(final int p_expect, final int p_iterations) {

        // Test
        for (int i = 0; i < p_iterations; i++) {
            Metadata.decrementSize(m_writer, m_reader, m_address);
        }

        Assert.assertEquals(p_expect, Metadata.getHashMapSize(m_reader, m_address));
    }

    public void end() {
        unpin();
    }
}

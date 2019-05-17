package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

/**
 * This class represents a response for on a MemoryInformationRequest
 *
 * @see de.hhu.bsinfo.dxnet.core.Request
 */
public class MemoryInformationResponse extends Response {

    private long m_allocated;
    private long m_used;
    private long m_metadata;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public MemoryInformationResponse() {
    }

    /**
     * Constructs a MemoryInformationResponse with requested information about the allocated memory and actually used
     * memory for a lot of ChunkIDs/Buckets.
     *
     * @param p_request   the request for this response
     * @param p_allocated entire size which was allocated
     * @param p_used      entire size which is actually used
     */
    public MemoryInformationResponse(final MemoryInformationRequest p_request, final long p_allocated, final long p_used,
                                     final long p_metadata) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_MEM_INFO_RES);
        m_allocated = p_allocated;
        m_used = p_used;
        m_metadata = p_metadata;
    }

    /**
     * Returns the entire allocated size.
     *
     * @returnthe entire allocated size.
     */
    public long getAllocated() {
        return m_allocated;
    }

    /**
     * Returns the entire size which is actually used.
     *
     * @return the entire size which is actually used.
     */
    public long getUsed() {
        return m_used;
    }

    /**
     * Returns the used memory for metadata
     *
     * @return the used memory for metadata
     */
    public long getMetadata() {
        return m_metadata;
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES * 3;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_allocated = p_importer.readLong(m_allocated);
        m_used = p_importer.readLong(m_used);
        m_metadata = p_importer.readLong(m_metadata);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_allocated);
        p_exporter.writeLong(m_used);
        p_exporter.writeLong(m_metadata);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MemoryInformationResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("\nBegin Data\nAllocated = ");
        builder.append(m_allocated);
        builder.append("\nUsed = ");
        builder.append(m_used);
        builder.append("\nEnd Data\n");

        return builder.toString();
    }
}
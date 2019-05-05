package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * This class represents a response on a AllocateChunkRequest for a chunk with a given size.
 *
 * @see de.hhu.bsinfo.dxnet.core.Response
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.AllocateChunkRequest
 */
public class AllocateChunkResponse extends Response {

    private long m_cid;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public AllocateChunkResponse() {
    }

    /**
     * Constructs a AllocateChunkResponse with the ChunkID from the requested allocation.
     *
     * @param p_request for the allocation.
     * @param p_cid     is the ChunkID from the requested Chunk.
     */
    public AllocateChunkResponse(Request p_request, final long p_cid) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES);
        m_cid = p_cid;
    }

    /**
     * Returns the ChunkID from the requested allocation.
     *
     * @return the ChunkID
     */
    public long getBucketCID() {
        return m_cid;
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_cid = p_importer.readLong(m_cid);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_cid);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AllocateChunkResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nCID = ");
        builder.append(ChunkID.toHexString(m_cid));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

}
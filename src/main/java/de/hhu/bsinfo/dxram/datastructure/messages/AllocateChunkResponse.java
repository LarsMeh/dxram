package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;

import java.util.Arrays;

public class AllocateChunkResponse extends Response {

    private long m_cid;

    public AllocateChunkResponse() {
        super();
    }

    public AllocateChunkResponse(Request p_request, final long p_cid) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES);
        m_cid = p_cid;
    }

    public long getBucketCID() {
        return m_cid;
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


}

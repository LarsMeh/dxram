package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;

public class PutResponse extends Response {

    private byte m_subsubtype;
    private long m_cid; // optional, only when bucket was split

    public PutResponse() {
        m_cid = ChunkID.INVALID_ID;
    }

    public PutResponse(final Request p_request, final byte p_subsubtype, final long p_cid) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_PUT_RES);
        m_subsubtype = p_subsubtype;
        if (validParameterCID())
            m_cid = p_cid;
        else
            m_cid = ChunkID.INVALID_ID;
    }

    private boolean validParameterCID() {
        return m_subsubtype == DataStructureMessageTypes.SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT ||
                m_subsubtype == DataStructureMessageTypes.SUBSUBTYPE_BUCKETSPLIT;
    }

    public byte getSubsubtype() {
        return m_subsubtype;
    }

    public long getCid() {
        if (validParameterCID())
            return m_cid;
        else
            return ChunkID.INVALID_ID;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PutResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nSubsubtype = ");
        builder.append(DataStructureMessageTypes.toString(m_subsubtype));
        builder.append("\nCid = ");
        builder.append(m_cid);
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

    @Override
    protected int getPayloadLength() {
        if (validParameterCID())
            return Byte.BYTES + Long.BYTES;
        else
            return Byte.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_subsubtype = p_importer.readByte(m_subsubtype);
        if (validParameterCID())
            m_cid = p_importer.readLong(m_cid);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_subsubtype);
        if (validParameterCID())
            p_exporter.writeLong(m_cid);
    }
}

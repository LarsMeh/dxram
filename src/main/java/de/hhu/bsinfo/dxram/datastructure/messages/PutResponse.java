package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * This class represents a response on a PutRequest.
 *
 * @see de.hhu.bsinfo.dxnet.core.Response
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.GetRequest
 */
public class PutResponse extends Response {

    private byte m_subsubtype;
    private long m_cid; // optional, only when bucket was split

    /**
     * Empty constructor which is needed for dxnet.
     * The ChunkId will be initialized as the invalid ChunkID which is {@value de.hhu.bsinfo.dxmem.data.ChunkID#INVALID_ID}.
     */
    public PutResponse() {
        m_cid = ChunkID.INVALID_ID;
    }

    /**
     * Constructs a PutResponse with a sub-sub-type from class {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes}.
     * Optional usage of a ChunkID will supported. The default value is {@value de.hhu.bsinfo.dxmem.data.ChunkID#INVALID_ID}.
     *
     * @param p_request    for the put operation.
     * @param p_subsubtype which is the result status from the put operation.
     * @param p_cid        optional. It indicates the bucket if a division has occurred.
     * @see de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes
     */
    public PutResponse(final Request p_request, final byte p_subsubtype, final long p_cid) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_PUT_RES);
        m_subsubtype = p_subsubtype;
        if (validParameterCID())
            m_cid = p_cid;
        else
            m_cid = ChunkID.INVALID_ID;
    }

    /**
     * Returns true if the ChunkID is valid.
     *
     * @return true if the ChunkID is valid.
     */
    private boolean validParameterCID() {
        return m_subsubtype == DataStructureMessageTypes.SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT ||
                m_subsubtype == DataStructureMessageTypes.SUBSUBTYPE_BUCKETSPLIT;
    }

    /**
     * Returns the sub-sub-type {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes}.
     *
     * @return the sub-sub-type {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes}.
     */
    public byte getSubsubtype() {
        return m_subsubtype;
    }

    /**
     * Returns the ChunkID. If it is invalid it will return {@value de.hhu.bsinfo.dxmem.data.ChunkID#INVALID_ID}.
     *
     * @return the ChunkID
     */
    public long getCid() {
        if (validParameterCID())
            return m_cid;
        else
            return ChunkID.INVALID_ID;
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
}

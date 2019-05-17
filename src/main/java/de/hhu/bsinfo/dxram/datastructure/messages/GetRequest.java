package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

/**
 * This class represents a request for a get operation on a ChunkID, which represents a bucket.
 *
 * @see de.hhu.bsinfo.dxnet.core.Request
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.GetResponse
 */
public class GetRequest extends Request {

    private byte[] m_key;
    private long m_cid;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public GetRequest() {
    }

    /**
     * Constructs a GetRequest for a key.
     *
     * @param p_destination target peer.
     * @param p_key         which identifies the key-value pair.
     * @param p_cid         ChunkID which stores the key-value pair.
     */
    public GetRequest(final short p_destination, final byte[] p_key, final long p_cid) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_REQ);
        m_key = p_key;
        m_cid = p_cid;
        setIgnoreTimeout(true);
    }

    /**
     * Returns the ChunkID of the bucket.
     *
     * @return the ChunkID of the bucket.
     */
    public long getCid() {
        return m_cid;
    }

    /**
     * Returns the key.
     *
     * @return the key.
     */
    public byte[] getKey() {
        return m_key;
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES + ObjectSizeUtil.sizeofByteArray(m_key);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_key = p_importer.readByteArray(m_key);
        m_cid = p_importer.readLong(m_cid);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_key);
        p_exporter.writeLong(m_cid);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("GetRequest\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("\nBegin Data\nKey = ");
        builder.append(Arrays.toString(m_key));
        builder.append("\nCID = ");
        builder.append(ChunkID.toHexString(m_cid));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }
}

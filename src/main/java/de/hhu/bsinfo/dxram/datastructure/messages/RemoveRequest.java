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
 * This class represents a request for a remove operation on a ChunkID, which represents a bucket.
 *
 * @see de.hhu.bsinfo.dxnet.core.Request
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.RemoveResponse
 * @see de.hhu.bsinfo.dxram.datastructure.messages.SignalResponse
 */
public class RemoveRequest extends Request {

    private byte[] m_key;
    private long m_cid;

    private byte[] m_value;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public RemoveRequest() {
    }

    /**
     * Constructs a RemoveRequest which a key and a ChunkID where the key-value pair is stored.
     * It uses the sub-type {@value de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes#SUBTYPE_REMOVE_REQ}.
     *
     * @param p_destination target peer.
     * @param p_key         key with which the specified value is to be associated.
     * @param p_cid         ChunkID where the key-value pair is stored.
     */
    public RemoveRequest(final short p_destination, final byte[] p_key, final long p_cid) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ);
        m_key = p_key;
        m_cid = p_cid;
    }

    /**
     * Constructs a RemoveRequest which a key and a ChunkID where the key-value pair is stored.
     * It uses the sub-type {@value de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes#SUBTYPE_REMOVE_WITH_KEY_REQ}.
     *
     * @param p_destination target peer.
     * @param p_key         key with which the specified value is to be associated.
     * @param p_value       value to be associated with the specified key.
     * @param p_cid         ChunkID where the key-value pair is stored.
     */
    public RemoveRequest(final short p_destination, final byte[] p_key, final byte[] p_value, final long p_cid) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ);
        m_key = p_key;
        m_cid = p_cid;
        m_value = p_value;
    }

    /**
     * Returns the key.
     *
     * @return the key.
     */
    public byte[] getKey() {
        return m_key;
    }

    /**
     * Returns the ChunkID where the key-value pair is stored.
     *
     * @return the ChunkID where the key-value pair is stored.
     */
    public long getCid() {
        return m_cid;
    }

    /**
     * Returns the value, but only when the sub-type is {@value de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes#SUBTYPE_REMOVE_WITH_KEY_REQ}.
     *
     * @return the value - but only when the sub-type is {@value de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes#SUBTYPE_REMOVE_WITH_KEY_REQ}, otherwise it returns null
     */
    public byte[] getValue() {
        return getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ ? m_value : null;
    }

    @Override
    protected int getPayloadLength() {
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_REQ)
            return Long.BYTES + ObjectSizeUtil.sizeofByteArray(m_key);
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ)
            return Long.BYTES + ObjectSizeUtil.sizeofByteArray(m_key) + ObjectSizeUtil.sizeofByteArray(m_value);

        return super.getPayloadLength();
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_cid = p_importer.readLong(m_cid);
        m_key = p_importer.readByteArray(m_key);
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ)
            m_value = p_importer.readByteArray(m_value);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_cid);
        p_exporter.writeByteArray(m_key);
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ)
            p_exporter.writeByteArray(m_value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(DataStructureMessageTypes.toString(getSubtype()) + "\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nKey = ");
        builder.append(Arrays.toString(m_key));
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ) {
            builder.append("\nValue = ");
            builder.append(Arrays.toString(m_value));
        }
        builder.append("\nCid = ");
        builder.append(ChunkID.toHexString(m_cid));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }
}

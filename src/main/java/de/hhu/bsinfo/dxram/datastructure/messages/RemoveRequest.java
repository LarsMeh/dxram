package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class RemoveRequest extends Request {

    private byte[] m_key;
    private long m_cid;

    private byte[] m_value;

    public RemoveRequest() {
    }

    public RemoveRequest(final short p_destination, final byte[] p_key, final long p_cid) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ);
        m_key = p_key;
        m_cid = p_cid;
    }

    public RemoveRequest(final short p_destination, final byte[] p_key, final byte[] p_value, final long p_cid) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ);
        m_key = p_key;
        m_cid = p_cid;
        m_value = p_value;
    }

    public byte[] getKey() {
        return m_key;
    }

    public long getCid() {
        return m_cid;
    }

    public byte[] getValue() {
        if (getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ)
            return m_value;
        else
            return null;
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
}

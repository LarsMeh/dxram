package de.hhu.bsinfo.dxram.datastructure.messages;


import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class PutRequest extends Request {

    private byte[] m_key;
    private byte[] m_value;
    private short m_tableDepth;
    private long m_cid;
    private byte m_hashFunctionId;

    public PutRequest() {
    }

    public PutRequest(final short p_destination, final byte[] p_key, final byte[] p_value,
                      final short p_tableDepth, final long p_cid, final byte p_hashFunctionId) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ);
        m_key = p_key;
        m_value = p_value;
        m_tableDepth = p_tableDepth;
        m_cid = p_cid;
        m_hashFunctionId = p_hashFunctionId;
    }

    public byte[] getKey() {
        return m_key;
    }

    public byte[] getValue() {
        return m_value;
    }

    public short getTableDepth() {
        return m_tableDepth;
    }

    public long getCid() {
        return m_cid;
    }

    public byte[] getHashedKey() {
        return HashFunctions.hash(m_hashFunctionId, m_key);
    }

    public byte getHashFunctionId() {
        return m_hashFunctionId;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PutRequest\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("\nBegin Data\nKey = ");
        builder.append(Arrays.toString(m_key));
        builder.append("\nValue = ");
        builder.append(Arrays.toString(m_value));
        builder.append("\nTable Depth = ");
        builder.append(m_tableDepth);
        builder.append("\nCid = ");
        builder.append(ChunkID.toHexString(m_cid));
        builder.append("\nhashFunctionId = ");
        builder.append(HashFunctions.toString(m_hashFunctionId));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES + Short.BYTES + Byte.BYTES + ObjectSizeUtil.sizeofByteArray(m_key) + ObjectSizeUtil.sizeofByteArray(m_value);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_key = p_importer.readByteArray(m_key);
        m_value = p_importer.readByteArray(m_value);
        m_tableDepth = p_importer.readShort(m_tableDepth);
        m_cid = p_importer.readLong(m_cid);
        m_hashFunctionId = p_importer.readByte(m_hashFunctionId);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_key);
        p_exporter.writeByteArray(m_value);
        p_exporter.writeShort(m_tableDepth);
        p_exporter.writeLong(m_cid);
        p_exporter.writeByte(m_hashFunctionId);
    }
}

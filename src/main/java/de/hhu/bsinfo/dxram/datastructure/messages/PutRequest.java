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

/**
 * This class represents a request for a put operation on a ChunkID, which represents a bucket.
 *
 * @see de.hhu.bsinfo.dxnet.core.Request
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.GetResponse
 */
public class PutRequest extends Request {

    private byte[] m_key;
    private byte[] m_value;
    private short m_tableDepth;
    private long m_cid;
    private byte m_hashFunctionId;
    private byte m_overwrite;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public PutRequest() {
    }

    /**
     * Constructs a PutRequest for a key-value pair.
     *
     * @param p_destination    target peer.
     * @param p_key            key with which the specified value is to be associated.
     * @param p_value          value to be associated with the specified key.
     * @param p_tableDepth     represents the depth of the hashtable.
     * @param p_cid            ChunkID where the key-value pair should be stored.
     * @param p_hashFunctionId which indicates the hash algorithm is used by the HashMap
     * @param p_overwrite      Indicates if overwrite is active or not
     */
    public PutRequest(final short p_destination, final byte[] p_key, final byte[] p_value, final short p_tableDepth,
                      final long p_cid, final byte p_hashFunctionId, final boolean p_overwrite) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ);
        m_key = p_key;
        m_value = p_value;
        m_tableDepth = p_tableDepth;
        m_cid = p_cid;
        m_hashFunctionId = p_hashFunctionId;
        m_overwrite = p_overwrite ? (byte) 1 : 0;
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
     * Returns the value.
     *
     * @return the value.
     */
    public byte[] getValue() {
        return m_value;
    }

    /**
     * Returns the depth of the hashtable.
     *
     * @return
     */
    public short getTableDepth() {
        return m_tableDepth;
    }

    /**
     * Returns the ChunkID where the key-value pair should be stored.
     *
     * @returnthe ChunkID where the key-value pair should be stored.
     */
    public long getCid() {
        return m_cid;
    }

    /**
     * Returns the hashed key.
     *
     * @return the hashed key.
     */
    public byte[] getHashedKey() {
        return HashFunctions.hash(m_hashFunctionId, m_key);
    }

    /**
     * Returns the id of the used hash algorithm.
     *
     * @return the id of the used hash algorithm.
     */
    public byte getHashFunctionId() {
        return m_hashFunctionId;
    }

    /**
     * Returns true if the overwrite is active.
     *
     * @return true if the overwrite is active.
     */
    public boolean getOverwrite() {
        return m_overwrite == 1;
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES + Short.BYTES + (Byte.BYTES * 2) + ObjectSizeUtil.sizeofByteArray(m_key) + ObjectSizeUtil.sizeofByteArray(m_value);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_key = p_importer.readByteArray(m_key);
        m_value = p_importer.readByteArray(m_value);
        m_tableDepth = p_importer.readShort(m_tableDepth);
        m_cid = p_importer.readLong(m_cid);
        m_hashFunctionId = p_importer.readByte(m_hashFunctionId);
        m_overwrite = p_importer.readByte(m_overwrite);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_key);
        p_exporter.writeByteArray(m_value);
        p_exporter.writeShort(m_tableDepth);
        p_exporter.writeLong(m_cid);
        p_exporter.writeByte(m_hashFunctionId);
        p_exporter.writeByte(m_overwrite);
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
        builder.append("\nOverwrite = ");
        builder.append((m_overwrite == 1 ? "activated" : "deactivated"));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

}

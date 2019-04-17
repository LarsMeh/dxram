package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class WriteBucketRawDataRequest extends Request {

    private long m_cid;
    private byte[] m_rawData;

    public WriteBucketRawDataRequest() {
        m_cid = ChunkID.INVALID_ID;
    }

    public WriteBucketRawDataRequest(final short p_destination, final long p_cid, final byte[] p_rawData) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ);
        m_cid = p_cid;
        m_rawData = p_rawData;
    }

    public byte[] getRawData() {
        return m_rawData;
    }

    public long getCid() {
        return m_cid;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("WriteBucketRawData\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nCID = ");
        builder.append(ChunkID.toHexString(m_cid));
        builder.append("\nRawData = ");
        builder.append(Arrays.toString(m_rawData));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

    @Override
    protected int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_rawData) + Long.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_cid = p_importer.readLong(m_cid);
        m_rawData = p_importer.readByteArray(m_rawData);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_cid);
        p_exporter.writeByteArray(m_rawData);
    }

}

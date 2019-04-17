package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class ClearMessage extends Message {

    private long[] m_cids;

    public ClearMessage() {
        super();
    }

    public ClearMessage(final short p_destination, final long[] p_cids) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE);
        m_cids = p_cids;
    }

    public long[] getCids() {
        return m_cids;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ClearMessage\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nData = ");
        builder.append(Arrays.toString(m_cids));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

    @Override
    protected int getPayloadLength() {
        return ObjectSizeUtil.sizeofLongArray(m_cids);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_cids = p_importer.readLongArray(m_cids);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLongArray(m_cids);
    }
}

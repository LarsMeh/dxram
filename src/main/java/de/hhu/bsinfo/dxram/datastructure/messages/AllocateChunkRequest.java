package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;

public class AllocateChunkRequest extends Request {

    private int m_initialBucketSize;


    public AllocateChunkRequest() {
    }

    public AllocateChunkRequest(short p_destination, final int p_initialBucketSize) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ);
        m_initialBucketSize = p_initialBucketSize;
    }

    public int getInitialBucketSize() {
        return m_initialBucketSize;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AllocateChunkRequest\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("\nBegin Data\nInitialSize = ");
        builder.append(m_initialBucketSize);
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

    @Override
    protected int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_initialBucketSize = p_importer.readInt(m_initialBucketSize);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_initialBucketSize);
    }
}

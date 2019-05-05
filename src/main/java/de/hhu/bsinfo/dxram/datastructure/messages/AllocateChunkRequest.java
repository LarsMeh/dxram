package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * This class represents a request for an allocation of a chunk with a given size.
 *
 * @see de.hhu.bsinfo.dxnet.core.Request
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.AllocateChunkResponse
 */
public class AllocateChunkRequest extends Request {

    private int m_initialBucketSize;


    /**
     * Empty constructor which is needed for dxnet.
     */
    public AllocateChunkRequest() {
    }

    /**
     * Constructs a AllocateChunkRequest with a size for the allocation.
     *
     * @param p_destination       target peer
     * @param p_initialBucketSize in bytes.
     */
    public AllocateChunkRequest(short p_destination, final int p_initialBucketSize) {
        super(p_destination, DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ);
        m_initialBucketSize = p_initialBucketSize;
    }

    /**
     * Returns the size for the requested allocation.
     *
     * @return the size for the requested allocation.
     */
    public int getInitialBucketSize() {
        return m_initialBucketSize;
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
}
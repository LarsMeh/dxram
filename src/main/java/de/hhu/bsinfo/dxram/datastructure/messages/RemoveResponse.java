package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

/**
 * This class represents a response on a RemoveRequest with sub-type {@value de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes#SUBTYPE_REMOVE_REQ}.
 *
 * @see de.hhu.bsinfo.dxnet.core.Response
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.RemoveRequest
 */
public class RemoveResponse extends Response {

    private byte[] m_value;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public RemoveResponse() {
    }

    /**
     * Constructs a RemoveResponse which the matching value to the key from the request.
     *
     * @param p_request for the remove operation.
     * @param p_value   matches to the key from the request.
     */
    public RemoveResponse(Request p_request, final byte[] p_value) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_REMOVE_RES);
        m_value = p_value;
    }

    /**
     * Returns the matching value to the key from the request.
     *
     * @return the matching value to the key from the request.
     */
    public byte[] getValue() {
        return m_value;
    }

    @Override
    protected int getPayloadLength() {
        return m_value != null ? ObjectSizeUtil.sizeofByteArray(m_value) : 0;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_value = p_importer.readByteArray(m_value);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RemoveResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nValue = ");
        builder.append(Arrays.toString(m_value));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }
}

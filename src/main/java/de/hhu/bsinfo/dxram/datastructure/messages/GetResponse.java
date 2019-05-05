package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

/**
 * This class represents a response on a GetRequest.
 *
 * @see de.hhu.bsinfo.dxnet.core.Response
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.GetRequest
 */
public class GetResponse extends Response {

    private byte[] m_value;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public GetResponse() {
    }

    /**
     * Constructs a GetResponse with the matching value. If the matching value not exists, it will be null.
     *
     * @param p_request for the get operation.
     * @param p_value   with matches to the key from the request or null.
     */
    public GetResponse(final Request p_request, final byte[] p_value) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_GET_RES);
        m_value = p_value;
    }

    /**
     * Returns the matching value or null.
     *
     * @return the matching value or null.
     */
    public byte[] getValue() {
        if (m_value.length != 0)
            return m_value;
        else
            return null;
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
        builder.append("GetResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nValue = ");
        builder.append(Arrays.toString(m_value));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }
}

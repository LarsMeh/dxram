package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;


/**
 * This class represents general response on a request where only a signal is needed.
 *
 * @see de.hhu.bsinfo.dxnet.core.Response
 * @see de.hhu.bsinfo.dxnet.core.Message
 * @see de.hhu.bsinfo.dxram.datastructure.messages.RemoveRequest
 * @see de.hhu.bsinfo.dxram.datastructure.messages.WriteBucketRawDataRequest
 */
public class SignalResponse extends Response {

    private byte m_signal;

    /**
     * Empty constructor which is needed for dxnet.
     */
    public SignalResponse() {
    }

    /**
     * Constructs a SignalResponse with a sub-sub-type from class {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes}.
     * The signal is equals to a sub-sub-type.
     *
     * @param p_request    for general operation.
     * @param p_subsubtype the signal.
     * @see de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes
     */
    public SignalResponse(Request p_request, byte p_subsubtype) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES);
        m_signal = p_subsubtype;
    }

    /**
     * Return true if the signal is successful. Otherwise it returns false.
     *
     * @return true if the signal is successful.
     */
    public boolean wasSuccessful() {
        return m_signal == DataStructureMessageTypes.SUBSUBTYPE_SUCCESS;
    }

    /**
     * Returns the signal.
     *
     * @return the signal.
     */
    public byte getSignal() {
        return m_signal;
    }

    @Override
    protected int getPayloadLength() {
        return Byte.BYTES;
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_signal = p_importer.readByte(m_signal);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_signal);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SignalResponse\nFrom:");
        builder.append(NodeID.toHexString(this.getSource()));
        builder.append("\nTo:");
        builder.append(NodeID.toHexString(this.getDestination()));
        builder.append("Begin Data\nSignal = ");
        builder.append(DataStructureMessageTypes.toString(m_signal));
        builder.append("\nEnd Data\n");

        return builder.toString();
    }

}

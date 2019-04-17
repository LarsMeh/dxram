package de.hhu.bsinfo.dxram.datastructure.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.NodeID;

public class SignalResponse extends Response {

    private byte m_signal;

    public SignalResponse() {
    }

    public SignalResponse(Request p_request, byte p_subsubtype) {
        super(p_request, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES);
        m_signal = p_subsubtype;
    }

    public boolean wasSuccessful() {
        return m_signal == DataStructureMessageTypes.SUBSUBTYPE_SUCCESS;
    }

    public byte getSignal() {
        return m_signal;
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


}

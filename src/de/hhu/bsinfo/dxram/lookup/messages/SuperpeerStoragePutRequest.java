/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Request to put data into the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStoragePutRequest extends AbstractRequest {
    // DataStructures used when sending the put request.
    // These are also used by the response to directly write the
    // receiving data to the structures
    // Chunks are created and used when receiving a put request
    private DataStructure m_dataStructure;

    // used when receiving message
    private DSByteArray m_chunk;

    private boolean m_isReplicate;

    /**
     * Creates an instance of SuperpeerStoragePutRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStoragePutRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStoragePutRequest
     *
     * @param p_destination
     *     the destination
     * @param p_dataStructure
     *     Data structure with the data to put.
     * @param p_replicate
     *     True if this message is a replication to other superpeer message, false if normal message
     */
    public SuperpeerStoragePutRequest(final short p_destination, final DataStructure p_dataStructure, final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST);

        m_dataStructure = p_dataStructure;
        m_isReplicate = p_replicate;
    }

    /**
     * Get the Chunks to put when this message is received.
     *
     * @return the Chunks to put
     */
    public final DSByteArray getChunk() {
        return m_chunk;
    }

    /**
     * Check if this request is a replicate message.
     *
     * @return True if replicate message.
     */
    public boolean isReplicate() {
        return m_isReplicate;
    }

    @Override
    protected final int getPayloadLength() {
        int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

        if (m_dataStructure != null) {
            size += Long.BYTES + Integer.BYTES + m_dataStructure.sizeofObject() + Byte.BYTES;
        } else {
            size += Long.BYTES + Integer.BYTES + m_chunk.sizeofObject() + Byte.BYTES;
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        int size = m_dataStructure.sizeofObject();

        p_buffer.putLong(m_dataStructure.getID());
        p_buffer.putInt(size);
        exporter.exportObject(m_dataStructure);
        p_buffer.put((byte) (m_isReplicate ? 1 : 0));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);

        m_chunk = new DSByteArray(p_buffer.getLong(), p_buffer.getInt());
        importer.importObject(m_chunk);
        m_isReplicate = p_buffer.get() != 0;
    }
}

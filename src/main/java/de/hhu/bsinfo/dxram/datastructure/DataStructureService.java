package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.datastructure.messages.*;
import de.hhu.bsinfo.dxram.engine.*;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

import java.io.File;

/**
 * This class represents the data structure service for dxram. It should be used to create instances of a data structure and remove them without damage the system.
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class DataStructureService extends Service<ModuleConfig> implements MessageReceiver {

    private short m_myNodeID;

    @Dependency
    private BootComponent m_boot;

    @Dependency
    private ChunkComponent m_chunk;

    @Dependency
    private NetworkComponent m_network;

    @Dependency
    private NameserviceComponent m_nameComponent;


    /**
     * Constructs a DataStructureService object.
     */
    public DataStructureService() {
    }

    /**
     * Creates a HashMap.
     *
     * @param p_name            the registration name for the {@link de.hhu.bsinfo.dxram.nameservice.NameserviceComponent}.
     * @param p_initialCapacity indicates the initial capacity of the HashMap.
     * @param p_numberOfNodes   indicates on how many peers the HashMap should be stored her buckets.
     * @param p_keyBytes        indicate how big is the key.
     * @param p_valueBytes      indicate how big is the value.
     * @param p_hashFunctionId  indicates which hash algorithm should be used.
     * @param <K>               Generic type of the key.
     * @param <V>               Generic type of the value.
     * @param p_NoOverwrite     Indicates if the HashMap should deactivate the overwrite function for better performance
     * @return An object of a HashMap.
     * @throws java.security.InvalidParameterException
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    public <K, V> HashMap<K, V> createHashMap(final String p_name, final int p_initialCapacity, final int p_numberOfNodes,
                                              final int p_keyBytes, final int p_valueBytes, final byte p_hashFunctionId,
                                              final boolean p_NoOverwrite) {

        return new HashMap<K, V>(m_chunk.getMemory(), this, p_name, p_initialCapacity, m_boot.getOnlinePeerIds(),
                p_numberOfNodes, (short) p_keyBytes, (short) p_valueBytes, p_hashFunctionId, p_NoOverwrite);
    }

    /**
     * Creates a HashMap on all online Peers.
     *
     * @param p_name            the registration name for the {@link de.hhu.bsinfo.dxram.nameservice.NameserviceComponent}.
     * @param p_initialCapacity indicates the initial capacity of the HashMap.
     * @param p_keyBytes        indicate how big is the key.
     * @param p_valueBytes      indicate how big is the value.
     * @param p_hashFunctionId  indicates which hash algorithm should be used.
     * @param <K>               Generic type of the key.
     * @param <V>               Generic type of the value.
     * @param p_NoOverwrite     Indicates if the HashMap should deactivate the overwrite function for better performance
     * @return An object of a HashMap.
     * @throws java.security.InvalidParameterException
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    public <K, V> HashMap<K, V> createHashMap(final String p_name, final int p_initialCapacity, final int p_keyBytes,
                                              final int p_valueBytes, final byte p_hashFunctionId, final boolean p_NoOverwrite) {

        return new HashMap<K, V>(m_chunk.getMemory(), this, p_name, p_initialCapacity, m_boot.getOnlinePeerIds(),
                (short) p_keyBytes, (short) p_valueBytes, p_hashFunctionId, p_NoOverwrite);
    }

    /**
     * Creates a HashMap with default hash algorithm.
     *
     * @param p_name            the registration name for the {@link de.hhu.bsinfo.dxram.nameservice.NameserviceComponent}.
     * @param p_initialCapacity indicates the initial capacity of the HashMap.
     * @param p_numberOfNodes   indicates on how many peers the HashMap should be stored her buckets.
     * @param p_keyBytes        indicate how big is the key.
     * @param p_valueBytes      indicate how big is the value.
     * @param <K>               Generic type of the key.
     * @param <V>               Generic type of the value.
     * @param p_NoOverwrite     Indicates if the HashMap should deactivate the overwrite function for better performance
     * @return An object of a HashMap.
     * @throws java.security.InvalidParameterException
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    public <K, V> HashMap<K, V> createHashMap(final String p_name, final int p_initialCapacity, final int p_numberOfNodes,
                                              final int p_keyBytes, final int p_valueBytes, final boolean p_NoOverwrite) {

        return new HashMap<K, V>(m_chunk.getMemory(), this, p_name, p_initialCapacity, m_boot.getOnlinePeerIds(),
                p_numberOfNodes, (short) p_keyBytes, (short) p_valueBytes, p_NoOverwrite);
    }

    /**
     * Creates a HashMap on all online Peers and with default hash algorithm.
     *
     * @param p_name            the registration name for the {@link de.hhu.bsinfo.dxram.nameservice.NameserviceComponent}.
     * @param p_initialCapacity indicates the initial capacity of the HashMap.
     * @param p_keyBytes        indicate how big is the key.
     * @param p_valueBytes      indicate how big is the value.
     * @param <K>               Generic type of the key.
     * @param <V>               Generic type of the value.
     * @param p_NoOverwrite     Indicates if the HashMap should deactivate the overwrite function for better performance
     * @return An object of a HashMap.
     * @throws java.security.InvalidParameterException
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    public <K, V> HashMap<K, V> createHashMap(final String p_name, final int p_initialCapacity, final int p_keyBytes,
                                              final int p_valueBytes, final boolean p_NoOverwrite) {

        return new HashMap<K, V>(m_chunk.getMemory(), this, p_name, p_initialCapacity, m_boot.getOnlinePeerIds(),
                (short) p_keyBytes, (short) p_valueBytes, p_NoOverwrite);
    }

    /**
     * Calls the package private method {@link de.hhu.bsinfo.dxram.datastructure.HashMap#remove}.
     *
     * @param p_hashMap HashMap which should be removed from DXRam
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap#remove
     */
    public void removeHashMap(final HashMap p_hashMap) {
        p_hashMap.removeThisObject();
    }


    /**
     * Calls the package private method {@link de.hhu.bsinfo.dxram.datastructure.HashMap#extractMemoryInformation}.
     *
     * @param p_hashMap HashMap which will inspected
     * @param p_file    File where the memory information will write to
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap#extractMemoryInformation
     */
    public void extractMemoryInformation(final HashMap p_hashMap, final File p_file) {
        if (p_file.exists()) {
            p_hashMap.extractMemoryInformation(p_file);
        }
    }

    /**
     * Register the ChunkID to a identifier.
     *
     * @param p_cid        ChunkID where the meta data or the data structure is stored to.
     * @param p_identifier for the data structure.
     */
    void registerDataStructure(final long p_cid, final String p_identifier) {
        m_nameComponent.register(p_cid, p_identifier);
    }

    /**
     * Unregister the ChunkID to a identifier.
     *
     * @param p_cid        ChunkID where the meta data or the data structure is stored to.
     * @param p_identifier for the data structure.
     */
    void unregisterDataStructure(final long p_cid, final String p_identifier) {
        // TODO: nameserivce component has no unregister method.
    }

    /**
     * Sends a synchronous request with the {@link de.hhu.bsinfo.dxram.net.NetworkComponent}.
     *
     * @param p_request which should be send.
     * @param p_timeout maximum duration.
     * @return the response of the request.
     */
    public Response sendSync(final Request p_request, final int p_timeout) {
        try {
            m_network.sendSync(p_request, p_timeout);
        } catch (NetworkException p_e) {
            LOGGER.error("NetworkInterface for HashMap catches a NetworkException for Request");
            p_e.printStackTrace();
        }

        return p_request.getResponse();
    }

    /**
     * Sends a response to synchronous request with the {@link de.hhu.bsinfo.dxram.net.NetworkComponent}.
     *
     * @param p_response of the request.
     */
    private void sendResponse(final Response p_response) {
        try {
            m_network.sendMessage(p_response);
        } catch (NetworkException p_e) {
            LOGGER.error("NetworkInterface for HashMap catches a NetworkException for Response");
            p_e.printStackTrace();
        }
    }

    /**
     * Send a asynchronous message with the {@link de.hhu.bsinfo.dxram.net.NetworkComponent}.
     *
     * @param p_message which should be send.
     */
    void sendAsync(final Message p_message) {
        try {
            m_network.sendMessage(p_message);
        } catch (NetworkException p_e) {
            p_e.printStackTrace();
        }
    }


    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handlePutRequest(final PutRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handlePutRequest(p_request, m_chunk.getMemory()));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handleWriteBucketRequest(final WriteBucketRawDataRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleWriteBucketRequest(p_request, m_chunk.getMemory()));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handleAllocateRequest(final AllocateChunkRequest p_request) {
        //LOGGER.warn(p_request.toString());
        long cid = m_chunk.getMemory().create().create(p_request.getInitialBucketSize());
        m_chunk.getMemory().pinning().pin(cid);
        sendResponse(new AllocateChunkResponse(p_request, cid));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     */
    private void handleRemoveRequest(final RemoveRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleRemoveRequest(p_request, m_chunk.getMemory()));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handleRemoveWithKeyRequest(final RemoveRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleRemoveWithKeyRequest(p_request, m_chunk.getMemory()));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handleGetRequest(final GetRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleGetRequest(p_request, m_chunk.getMemory()));
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_message for an operation on this peer.
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap
     */
    private void handleClearMessage(final ClearMessage p_message) {
        //LOGGER.warn(p_message.toString());
        HashMap.handleClearRequest(p_message, m_chunk.getMemory());
    }

    /**
     * Calls the handle method from the matching data structure.
     *
     * @param p_request request for a memory information
     */
    private void handleMemoryInformationRequest(final MemoryInformationRequest p_request) {
        //LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleMemoryInformationRequest(p_request, m_chunk.getMemory()));
    }


    /**
     * Returns true if the given nodeId is equal to the nodeId of this instance.
     *
     * @param p_suspect nodeId.
     * @return true if the given nodeId is equal to the nodeId of this instance.
     */
    public boolean isLocal(final short p_suspect) {
        return m_myNodeID == p_suspect;
    }

    /**
     * Returns true if the given nodeId is equal to the nodeId of this instance.
     *
     * @param p_suspect nodeId.
     * @return true if the given nodeId is equal to the nodeId of this instance.
     */
    public boolean isLocal(final long p_suspect) {
        return m_myNodeID == ChunkID.getCreatorID(p_suspect);
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        m_myNodeID = m_boot.getNodeId();

        registerNetworkMessages();
        registerNetworkMessageListener();

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }


    @Override
    public void onIncomingMessage(Message p_message) {

        if (p_message.getType() == DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE) {

            switch (p_message.getSubtype()) {
                case DataStructureMessageTypes.SUBTYPE_PUT_REQ:
                    handlePutRequest((PutRequest) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ:
                    handleAllocateRequest((AllocateChunkRequest) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ:
                    handleWriteBucketRequest((WriteBucketRawDataRequest) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_REMOVE_REQ:
                    handleRemoveRequest((RemoveRequest) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ:
                    handleRemoveWithKeyRequest((RemoveRequest) p_message);
                case DataStructureMessageTypes.SUBTYPE_GET_REQ:
                    handleGetRequest((GetRequest) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE:
                    handleClearMessage((ClearMessage) p_message);
                    break;
                case DataStructureMessageTypes.SUBTYPE_MEM_INFO_REQ:
                    handleMemoryInformationRequest((MemoryInformationRequest) p_message);
                default:
                    break;
            }

        } else
            LOGGER.warn("MessageType unknown. Message could not be handled");
    }

    /**
     * Register all sub-types from class {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes} to the network component.
     *
     * @see de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes
     * @see de.hhu.bsinfo.dxram.DXRAMMessageTypes
     * @see de.hhu.bsinfo.dxram.net.NetworkComponent
     */
    private void registerNetworkMessages() {
        // Request
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ, PutRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ, AllocateChunkRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ, WriteBucketRawDataRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ, RemoveRequest.class); // TODO erlaubt?
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ, RemoveRequest.class); // TODO erlaubt?
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_REQ, GetRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_MEM_INFO_REQ, MemoryInformationRequest.class);

        // Response
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_RES, PutResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES, AllocateChunkResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES, SignalResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_RES, RemoveResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_RES, GetResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_MEM_INFO_RES, MemoryInformationResponse.class);

        // Messages
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE, ClearMessage.class);
    }

    /**
     * Register a listener for all sub-types from class {@link de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes} by the network component.
     *
     * @see de.hhu.bsinfo.dxram.datastructure.messages.DataStructureMessageTypes
     * @see de.hhu.bsinfo.dxram.DXRAMMessageTypes
     * @see de.hhu.bsinfo.dxram.net.NetworkComponent
     */
    private void registerNetworkMessageListener() {
        // Request
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_MEM_INFO_REQ, this);

        // Response
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_MEM_INFO_RES, this);

        // Messages
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE, this);
    }

}

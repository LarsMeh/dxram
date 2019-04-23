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
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import de.hhu.bsinfo.dxram.engine.*;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

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


    public DataStructureService() {
    }

    /*** Create data structures ***/
    public <K, V> HashMap<K, V> createHashMap(final String p_name, final int p_initialCapacity, final int p_numberOfNodes,
                                              final int p_keyBytes, final int p_valueBytes, final byte p_hashFunctionId) {
        assert p_initialCapacity > 1 && (p_numberOfNodes > 0 || p_numberOfNodes == -1)&& (short) p_keyBytes > 0 && (short) p_valueBytes > 0 &&
                HashFunctions.isProperValue(p_hashFunctionId);

        return new HashMap<K, V>(m_chunk.getMemory(), this, p_name, p_initialCapacity, m_boot.getOnlinePeerIds(),
                p_numberOfNodes, (short) p_keyBytes, (short) p_valueBytes, p_hashFunctionId);
    }

    /*** Registration ***/
    void registerHashMap(final long p_cid, final String p_identifier) {
        m_nameComponent.register(p_cid, p_identifier);
    }

    /*** Network Communication ***/
    public Response sendSync(final Request p_request, final int p_timeout) {
        try {
            m_network.sendSync(p_request, p_timeout);
        } catch (NetworkException p_e) {
            LOGGER.error("NetworkInterface for HashMap catches a NetworkException for Request");
            p_e.printStackTrace();
        }

        return p_request.getResponse();
    }

    private void sendResponse(final Response p_response) {
        try {
            m_network.sendMessage(p_response);
        } catch (NetworkException p_e) {
            LOGGER.error("NetworkInterface for HashMap catches a NetworkException for Response");
            p_e.printStackTrace();
        }
    }

    void sendAsync(final Message p_message) {
        try {
            m_network.sendMessage(p_message);
        } catch (NetworkException p_e) {
            p_e.printStackTrace();
        }
    }


    /**
     * Handle Network Messages
     **/
    private void handlePutRequest(final PutRequest p_request) {
        LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handlePutRequest(p_request, m_chunk.getMemory()));
    }

    private void handleWriteBucketRequest(final WriteBucketRawDataRequest p_request) {
        LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleWriteBucketRequest(p_request, m_chunk.getMemory()));
    }

    private void handleAllocateRequest(final AllocateChunkRequest p_request) {
        LOGGER.warn(p_request.toString());
        long cid = m_chunk.getMemory().create().create(p_request.getInitialBucketSize());
        sendResponse(new AllocateChunkResponse(p_request, cid));
    }

    private void handleRemoveRequest(final RemoveRequest p_request) {
        LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleRemoveRequest(p_request, m_chunk.getMemory()));
    }

    private void handleRemoveWithKeyRequest(final RemoveRequest p_request) {
        LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleRemoveWithKeyRequest(p_request, m_chunk.getMemory()));
    }

    private void handleGetRequest(final GetRequest p_request) {
        LOGGER.warn(p_request.toString());
        sendResponse(HashMap.handleGetRequest(p_request, m_chunk.getMemory()));
    }

    private void handleClearMessage(final ClearMessage p_message) {
        LOGGER.warn(p_message.toString());
        HashMap.handleClearRequest(p_message, m_chunk.getMemory());
    }


    /*** Peer Info's ***/
    public boolean isLocal(final short p_suspect) {
        //LOGGER.info("isLocal call Success=" + (m_myNodeID == p_suspect));
        return m_myNodeID == p_suspect;
    }

    public boolean isLocal(final long p_suspect) {
        //LOGGER.info("isLocal call Success=" + (m_myNodeID == ChunkID.getCreatorID(p_suspect)));
        return m_myNodeID == ChunkID.getCreatorID(p_suspect);
    }

    /*** Override as a Service ***/

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
                default:
                    break;
            }

        } else
            LOGGER.warn("MessageType unknown. Message could not be handled");
    }

    private void registerNetworkMessages() {
        // Request
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ, PutRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ, AllocateChunkRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ, WriteBucketRawDataRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ, RemoveRequest.class); // TODO erlaubt?
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ, RemoveRequest.class); // TODO erlaubt?
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_REQ, GetRequest.class);

        // Response
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_RES, PutResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES, AllocateChunkResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES, SignalResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_RES, RemoveResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_RES, GetResponse.class);

        // Messages
        m_network.registerMessageType(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE, ClearMessage.class);
    }

    private void registerNetworkMessageListener() {
        // Request
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_REQ, this);

        // Response
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_PUT_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_ALLOCATE_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_SIGNAL_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_REMOVE_RES, this);
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_GET_RES, this);

        // Messages
        m_network.register(DXRAMMessageTypes.DATA_STRUCTURE_MESSAGE_TYPE, DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE, this);
    }

}

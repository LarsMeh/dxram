package de.hhu.bsinfo.dxram.datastructure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.primitives.Longs;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxmem.operations.Size;
import de.hhu.bsinfo.dxram.datastructure.messages.*;
import de.hhu.bsinfo.dxram.datastructure.util.*;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.skema.Skema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * A synchronized HashMap for distributed In-memory store. Its based on a variation of extendible hashing. In contrast
 * to {@link java.util.HashMap} it permits no null values.
 * For the serialization the class {@link de.hhu.bsinfo.skema.Skema} is used. It means that only data types which could
 * serialized with skema, could be supported. The only exception is type byte[]. The HashMap will switch into
 * another mode for key and/or value.
 * <p>
 * The HashMap uses a {@link java.util.concurrent.locks.ReentrantReadWriteLock} to synchronize the operation. Methods
 * {@link #put(java.lang.Object, java.lang.Object)}, {@link #clear()} uses a write lock.
 * <p>
 * The HashMap can only initialized from the {@link de.hhu.bsinfo.dxram.datastructure.DataStructureService}. If you dont
 * need longer he object remove it also with the {@link de.hhu.bsinfo.dxram.datastructure.DataStructureService#removeHashMap(HashMap p_hashMap)}.
 *
 * @param <K> Generic type for Key
 * @param <V> Generic type for Value
 * @see de.hhu.bsinfo.dxmem.DXMem
 * @see de.hhu.bsinfo.dxmem.operations.RawWrite
 * @see de.hhu.bsinfo.dxmem.operations.RawRead
 * @see de.hhu.bsinfo.dxmem.operations.Pinning
 * @see de.hhu.bsinfo.skema.Skema
 * @see de.hhu.bsinfo.dxram.datastructure.DataStructureService
 */
public class HashMap<K, V> {

    private static final Logger log = LogManager.getFormatterLogger(HashMap.class);

    private static final int BUCKET_ENTRIES_EXP;
    private static final int HASH_TABLE_DEPTH_LIMIT;
    static final int BUCKET_ENTRIES;
    private static final short BUCKET_INITIAL_DEPTH;
    private static final short SKEMA_DEFAULT_ID;
    private static final short MAX_DEPTH;

    static {
        HASH_TABLE_DEPTH_LIMIT = 31;
        BUCKET_ENTRIES_EXP = 3;
        BUCKET_ENTRIES = (int) Math.pow(2, BUCKET_ENTRIES_EXP);
        BUCKET_INITIAL_DEPTH = 0;
        SKEMA_DEFAULT_ID = -1;
        MAX_DEPTH = 27;
        Skema.enableAutoRegistration();
    }


    private final ReentrantReadWriteLock m_lock;

    private final DataStructureService m_service;
    private final DXMem m_memory;
    private final RawRead m_reader;
    private final RawWrite m_writer;
    private final Pinning m_pinning;

    private final long m_metaDataCID; // should be recovered
    private final long m_metaDataAdr; // should be recovered

    private final long m_hashtableCID; // should be recovered
    private long m_hashtableAdr; // should be recovered

    private final long m_nodepool_cid; // should be recovered

    private final byte m_hashFunctionId; // should be recovered

    private boolean m_skemaRegistrationSwitch; // TODO: Put into metadata
    private boolean m_serializeKey; // TODO: Put into metadata
    private boolean m_serializeValue; // TODO: Put into metadata
    private final boolean m_overwrite;

    /**
     * Constructs an empty HashMap an will distribute them on all online peers.
     *
     * @param p_memory          an instance of DXMem to get direct memory access
     * @param p_service         for network communication
     * @param p_name            identifier for the metadata chunk
     * @param p_initialCapacity minimum capacity of this instance to store key-value pairs
     * @param p_onlinePeers     list of all online peers (no superpeers) in dxram
     * @param p_keyBytes        size of a key
     * @param p_valueBytes      size of a value
     * @param p_hashFunctionId  id for the hash algorithm which should be used
     */
    HashMap(final DXMem p_memory, final DataStructureService p_service, final String p_name, final int p_initialCapacity,
            final List<Short> p_onlinePeers, final short p_keyBytes, final short p_valueBytes, final byte p_hashFunctionId,
            final boolean p_NoOverwrite) {
        this(p_memory, p_service, p_name, p_initialCapacity, p_onlinePeers, -1, p_keyBytes, p_valueBytes,
                p_hashFunctionId, p_NoOverwrite);
    }

    /**
     * Constructs an empty HashMap with default hash algorithm {@link de.hhu.bsinfo.dxram.datastructure.util.HashFunctions#MURMUR3_32}.
     *
     * @param p_memory          an instance of DXMem to get direct memory access
     * @param p_service         for network communication
     * @param p_name            identifier for the metadata chunk
     * @param p_initialCapacity minimum capacity of this instance to store key-value pairs
     * @param p_onlinePeers     list of all online peers (no superpeers) in dxram
     * @param p_numberOfNodes   number of peers, where the HashMap should be distributed their buckets
     * @param p_keyBytes        size of a key
     * @param p_valueBytes      size of a value
     */
    HashMap(final DXMem p_memory, final DataStructureService p_service, final String p_name, final int p_initialCapacity,
            final List<Short> p_onlinePeers, final int p_numberOfNodes, final short p_keyBytes, final short p_valueBytes,
            final boolean p_NoOverwrite) {
        this(p_memory, p_service, p_name, p_initialCapacity, p_onlinePeers, p_numberOfNodes, p_keyBytes, p_valueBytes,
                HashFunctions.MURMUR3_32, p_NoOverwrite);
    }

    /**
     * Constructs an empty HashMap with default hash algorithm {@link de.hhu.bsinfo.dxram.datastructure.util.HashFunctions#MURMUR3_32}
     * and will distribute them on all online peers.
     *
     * @param p_memory          an instance of DXMem to get direct memory access
     * @param p_service         for network communication
     * @param p_name            identifier for the metadata chunk
     * @param p_initialCapacity minimum capacity of this instance to store key-value pairs
     * @param p_onlinePeers     list of all online peers (no superpeers) in dxram
     * @param p_keyBytes        size of a key
     * @param p_valueBytes      size of a value
     */
    HashMap(final DXMem p_memory, final DataStructureService p_service, final String p_name, final int p_initialCapacity,
            final List<Short> p_onlinePeers, final short p_keyBytes, final short p_valueBytes, final boolean p_NoOverwrite) {
        this(p_memory, p_service, p_name, p_initialCapacity, p_onlinePeers, -1, p_keyBytes, p_valueBytes,
                HashFunctions.MURMUR3_32, p_NoOverwrite);
    }

    /**
     * Constructs a empty HashMap with given parameter. The metadata ChunkID will be register with parameter p_name.
     *
     * @param p_memory          an instance of DXMem to get direct memory access
     * @param p_service         for network communication
     * @param p_name            identifier for the metadata chunk
     * @param p_initialCapacity minimum capacity of this instance to store key-value pairs
     * @param p_onlinePeers     list of all online peers (no superpeers) in dxram
     * @param p_numberOfNodes   number of peers, where the HashMap should be distributed their buckets
     * @param p_keyBytes        size of a key
     * @param p_valueBytes      size of a value
     * @param p_hashFunctionId  id for the hash algorithm which should be used
     * @see de.hhu.bsinfo.dxmem
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Pinning
     * @see de.hhu.bsinfo.dxram.datastructure.DataStructureService
     */
    HashMap(final DXMem p_memory, final DataStructureService p_service, final String p_name, final int p_initialCapacity,
            final List<Short> p_onlinePeers, final int p_numberOfNodes, final short p_keyBytes, final short p_valueBytes,
            final byte p_hashFunctionId, final boolean p_NoOverwrite) {
        if (!HashMap.assertInitialParameter(p_name, p_initialCapacity, p_onlinePeers, p_numberOfNodes, p_keyBytes,
                p_valueBytes, p_hashFunctionId))
            throw new InvalidParameterException();

        m_lock = new ReentrantReadWriteLock(true);

        m_service = p_service;
        m_memory = p_memory;
        m_reader = m_memory.rawRead();
        m_writer = m_memory.rawWrite();
        m_pinning = m_memory.pinning();

        m_hashFunctionId = p_hashFunctionId;
        m_skemaRegistrationSwitch = true;
        m_serializeKey = true;
        m_serializeValue = true;
        m_overwrite = !p_NoOverwrite;

        short hashtable_depth = calcTableDepth(p_initialCapacity); // calculate hashtable_depth

        m_nodepool_cid = initNodePool(p_onlinePeers, p_numberOfNodes); // Init NodePool

        long bucket_cid = initBucket(BUCKET_INITIAL_DEPTH, HashMap.BUCKET_ENTRIES, p_keyBytes, p_valueBytes); // Init Bucket

        m_hashtableCID = initHashtable(hashtable_depth, bucket_cid); // Init Hashtable
        m_hashtableAdr = m_memory.pinning().translate(m_hashtableCID);

        m_metaDataCID = initMetaData(m_nodepool_cid, m_hashtableCID, Bucket.getInitialMemorySize(BUCKET_ENTRIES, p_keyBytes, p_valueBytes), p_hashFunctionId); // Init Metadata
        m_metaDataAdr = m_memory.pinning().translate(m_metaDataCID);

        m_service.registerDataStructure(m_metaDataCID, p_name); // Register Metadata
    }

    /**
     * Removes all pinned and used ChunkIDs from dxram. After this method is called no operation should be called for
     * the same object.
     * By using this method while more than one thread calls method on the same object take care that this method is not
     * called while another thread is in a locked method of this object.
     *
     * @see de.hhu.bsinfo.dxmem.operations.Remove
     * @see de.hhu.bsinfo.dxmem.operations.Pinning
     * @see de.hhu.bsinfo.dxram.datastructure.DataStructureService#removeHashMap(HashMap p_hashMap)
     */
    void removeThisObject() {
        m_lock.writeLock().lock(); // lock reentrant write lock

        this.clear(false);

        m_memory.pinning().unpinCID(m_nodepool_cid);
        m_memory.remove().remove(m_nodepool_cid);

        m_memory.pinning().unpinCID(m_hashtableAdr);
        m_memory.remove().remove(m_hashtableCID);

        m_memory.pinning().unpinCID(m_metaDataAdr);
        m_memory.remove().remove(m_metaDataCID);

        m_lock.writeLock().unlock(); // unlock reentrant write lock
    }

    /**
     * Initializes the memory layout {@link de.hhu.bsinfo.dxram.datastructure.NodePool} and returns the ChunkID of
     * the NodePool.
     *
     * @param p_onlinePeers   list of all online peers (no superpeers) in dxram
     * @param p_numberOfNodes number of peers, where the HashMap should be distributed their buckets
     * @return the ChunkID of the NodePool.
     */
    private long initNodePool(@NotNull final List<Short> p_onlinePeers, final int p_numberOfNodes) {
        long cid = m_memory.create().create(NodePool.getInitialMemorySize(p_onlinePeers.size(), p_numberOfNodes));
        long address = pin(cid);

        NodePool.initialize(m_writer, p_onlinePeers, address, p_numberOfNodes);

        return cid;
    }

    /**
     * Initializes the memory layout {@link de.hhu.bsinfo.dxram.datastructure.Bucket} and returns the ChunkID of
     * the Bucket.
     *
     * @param p_depth      initial depth of the bucket
     * @param p_entries    Maximum number of key-value pair which should the bucket store
     * @param p_keyBytes   size of a key
     * @param p_valueBytes size of a value
     * @return the ChunkID of the Bucket.
     */
    private long initBucket(final short p_depth, final int p_entries, final short p_keyBytes, final short p_valueBytes) {
        int size = Bucket.getInitialMemorySize(p_entries, p_keyBytes, p_valueBytes);
        long cid = m_memory.create().create(size);
        long address = pin(cid);

        Bucket.initialize(m_writer, address, p_depth);

        return cid;
    }

    /**
     * Initializes the memory layout {@link de.hhu.bsinfo.dxram.datastructure.Hashtable} and returns the ChunkID of
     * the Hashtable.
     *
     * @param p_depth        initial depth of the hashtable
     * @param p_defaultEntry for the hashtable
     * @return the ChunkID of the Hashtable.
     */
    private long initHashtable(final short p_depth, final long p_defaultEntry) {
        int initialSize = Hashtable.getInitialMemorySize(p_depth);
        long cid = m_memory.create().create(initialSize);
        long address = pin(cid);

        Hashtable.initialize(m_writer, address, initialSize, p_depth, p_defaultEntry);

        return cid;
    }

    /**
     * Initializes the memory layout {@link de.hhu.bsinfo.dxram.datastructure.Metadata} and returns the ChunkID of
     * the Metadata.
     *
     * @param p_nodePool_cid          ChunkID of the NodePool
     * @param p_hashtable_cid         ChunkID of the Hashtable
     * @param p_individual_bucketSize individual size of the bucket based on the size of key and value
     * @param p_hashFunctionId        id for the hash algorithm which will be used
     * @return the ChunkID of the Metadata.
     */
    private long initMetaData(final long p_nodePool_cid, final long p_hashtable_cid, final int p_individual_bucketSize,
                              final byte p_hashFunctionId) {
        long cid = m_memory.create().create(Metadata.getInitialMemorySize());
        long address = pin(cid);

        Metadata.initialize(m_writer, address, p_hashtable_cid, p_nodePool_cid, 0L, SKEMA_DEFAULT_ID, p_individual_bucketSize, p_hashFunctionId);

        return cid;
    }

    /**
     * Returns the matching virtual address for the ChunkID by pinning them. This method is not synchronized or atomic.
     * It calls {@link #pin(DXMem, long)} with local instance of dxmem.
     *
     * @param p_cid ChunkID which will be pinned
     * @return the matching virtual address for parameter p_cid.
     * @see #pin(DXMem, long)
     */
    private long pin(final long p_cid) {
        return pin(m_memory, p_cid);
    }

    /**
     * Returns the matching virtual address for the ChunkID by pinning them. This method is not synchronized or atomic.
     *
     * @param p_memory access to pinning
     * @param p_cid    ChunkID which should be pinned
     * @return the matching virtual address for the ChunkID.
     * @throws java.lang.RuntimeException
     */
    private static long pin(@NotNull final DXMem p_memory, final long p_cid) { // TODO: pinning is ok not dxmem and throws clausel
        Pinning.PinnedMemory pinnedMemory = p_memory.pinning().pin(p_cid);

        if (!pinnedMemory.isStateOk()) {
            log.error("CID = " + p_cid + " could not be pinned. State = " + pinnedMemory.getState().toString());
            throw new RuntimeException();
        }

        return pinnedMemory.getAddress();
    }

    /**
     * This class represents a complex return type for methods.
     */
    private static class Result {

        private boolean m_success = false; // true if method was successful
        private long m_new_bucket = ChunkID.INVALID_ID; // ChunkID when bucket got split
        private boolean m_resized = false; // true if a resize is necessary
        private boolean m_error = false; // true if an error occurred during the method
        private boolean m_overwrite = false; // true if a key exists and value was overwrite

        /**
         * Only used if u checked sub-sub-type. It returns the correct subsubtype based on the local fields of this object.
         *
         * @return the correct subsubtype based on the local fields of this object.
         */
        private byte getSubSubType() {
            if (m_success) {
                if (m_overwrite && m_new_bucket == ChunkID.INVALID_ID)
                    return DataStructureMessageTypes.SUBSUBTYPE_OVERWRITE;
                else if (m_overwrite)
                    return DataStructureMessageTypes.SUBSUBTYPE_OVERWRITE_AND_BUCKETSPLIT;
                else if (m_new_bucket == ChunkID.INVALID_ID)
                    return DataStructureMessageTypes.SUBSUBTYPE_SUCCESS;
                else
                    return DataStructureMessageTypes.SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT;
            } else {
                if (m_resized)
                    return DataStructureMessageTypes.SUBSUBTYPE_RESIZE;
                else if (m_new_bucket != ChunkID.INVALID_ID)
                    return DataStructureMessageTypes.SUBSUBTYPE_BUCKETSPLIT;
                else
                    return DataStructureMessageTypes.SUBSUBTYPE_ERROR;
            }
        }
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping
     * for the key, the old value is replaced. Null values are not supported.
     *
     * @param p_key   key with which the specified value is to be associated
     * @param p_value value to be associated with the specified key
     * @return true if the key-value pair could be inserted.
     * @throws java.lang.NullPointerException
     * @throws de.hhu.bsinfo.dxmem.core.MemoryRuntimeException
     */
    public boolean put(final K p_key, final V p_value) {
        assert p_key != null && p_value != null;
        byte[] key, value;
        HashMap.Result result;
        short depth;

        if (m_skemaRegistrationSwitch) { // Skema registration
            if (p_key.getClass() == byte[].class)
                m_serializeKey = false;
            if (p_value.getClass() == byte[].class)
                m_serializeValue = false;
            else
                Metadata.setSkemaValueType(m_writer, m_metaDataAdr, Skema.resolveIdentifier(p_value.getClass()));

            m_skemaRegistrationSwitch = false;
        }


        if (m_serializeKey) // Serialize
            key = Skema.serialize(p_key);
        else
            key = (byte[]) p_key;

        if (m_serializeValue)
            value = Skema.serialize(p_value);
        else
            value = (byte[]) p_value;

        final byte[] hash = HashFunctions.hash(m_hashFunctionId, key);

        m_lock.writeLock().lock(); // reentrant write lock

        depth = Hashtable.getDepth(m_reader, m_hashtableAdr);
        int index = ExtendibleHashing.extendibleHashing(hash, depth);
        long cid = Hashtable.lookup(m_reader, m_hashtableAdr, index); // Lookup in Hashtable

        do {

            if (m_service.isLocal(cid)) { // bucket is local

                long address = m_pinning.translate(cid);
                result = putLocal(address, cid, hash, key, value, m_overwrite);

            } else { // bucket is global

                PutRequest request = new PutRequest(ChunkID.getCreatorID(cid), key, value, depth, cid, m_hashFunctionId, m_overwrite);
                PutResponse response = (PutResponse) m_service.sendSync(request, -1);
                result = consumePutResponse(response);

                if (result.m_error)
                    log.error("Put Request throws an error");

            }

            if (result.m_error) { // handle error

                m_lock.writeLock().unlock();

                log.error("Could not put key into HashMap\nKey = " + Arrays.toString(key));

                return false;

            }

            if (result.m_resized) { // handle optional resize

                if (depth == MAX_DEPTH) {

                    m_lock.writeLock().unlock();

                    log.error("Hashtable reached maximum depth of " + MAX_DEPTH);

                    return false;

                } else {

                    if (m_memory.stats().getHeapStatus().getTotalSizeBytes() < ((long) Math.pow(2, depth + 1) * Long.BYTES + 2))
                        throw new MemoryRuntimeException("Total Bytes: " + m_memory.stats().getHeapStatus().getTotalSizeBytes() + " But want to allocate more");

                    depth++;
                    m_hashtableAdr = Hashtable.resize(m_reader, m_writer, m_memory.resize(), m_pinning, m_hashtableCID, m_hashtableAdr);
                    index = ExtendibleHashing.extendibleHashing(hash, depth);
                    cid = Hashtable.lookup(m_reader, m_hashtableAdr, index);

                }

            }

            if (result.m_new_bucket != ChunkID.INVALID_ID) {  // handle optional bucket split

                Hashtable.splitForEntry(m_reader, m_writer, m_memory.size(), m_hashtableCID, m_hashtableAdr, cid, index, result.m_new_bucket);
                cid = Hashtable.lookup(m_reader, m_hashtableAdr, index);

            }

            if (result.m_success && !result.m_overwrite) // Metadata increment size
                Metadata.incrementSize(m_writer, m_reader, m_metaDataAdr);

        } while (!result.m_success);

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        return true;
    }

//    public long allocate(final int p_size) {
//        return m_memory.create().create(p_size);
//    }

    /**
     * Returns a result object based on the sub-sub-type of the response message.
     *
     * @param p_response response message on a put request
     * @return a result object based on the sub-sub-type of the response message.
     */
    private HashMap.Result consumePutResponse(@NotNull final PutResponse p_response) {
        HashMap.Result result = new HashMap.Result();

        switch (p_response.getSubsubtype()) {
            case DataStructureMessageTypes.SUBSUBTYPE_SUCCESS:
                result.m_success = true;
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT:
                result.m_success = true;
                result.m_new_bucket = p_response.getCid();
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_BUCKETSPLIT:
                result.m_new_bucket = p_response.getCid();
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_RESIZE:
                result.m_resized = true;
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_ERROR:
                result.m_error = true;
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_OVERWRITE_AND_BUCKETSPLIT:
                result.m_overwrite = true;
                result.m_success = true;
                result.m_new_bucket = p_response.getCid();
                break;
            case DataStructureMessageTypes.SUBSUBTYPE_OVERWRITE:
                result.m_overwrite = true;
                result.m_success = true;
                break;
            default:
                log.error("Unknown SubSubtype: " + p_response.getSubsubtype() + " - DSMT: " + DataStructureMessageTypes.toString(p_response.getSubsubtype()));
                throw new RuntimeException();
        }

        return result;
    }

    /**
     * Returns a result object based on the executed operation and its result.
     *
     * @param p_address   address where the bucket is stored
     * @param p_bucketCID ChunkID of the bucket
     * @param p_hash      hashed key
     * @param p_key       key with which the specified value is to be associated
     * @param p_value     value to be associated with the specified key
     * @return a result object based on the executed operation and its results.
     * @throws java.lang.RuntimeException
     */
    private HashMap.Result putLocal(final long p_address, final long p_bucketCID, final byte[] p_hash, final byte[] p_key,
                                    final byte[] p_value, final boolean p_overwrite) {
        if (!Bucket.isFull(m_reader, p_address)) { // Bucket has space

            HashMap.Result result = savePut(m_memory, p_address, p_bucketCID, p_key, p_value, p_overwrite);

            if (!result.m_success)
                log.warn("SavePut failed for key: " + Arrays.toString(p_key));

            return result;

        } else { // Bucket has maximum entries

            short bucket_depth = Bucket.getDepth(m_reader, p_address);

            int table_depth = Hashtable.getDepth(m_reader, m_hashtableAdr);

            if (bucket_depth < table_depth) { // true - split bucket

                return splitBucketAndPut(p_address, p_bucketCID, p_hash, m_hashFunctionId, p_key, p_value, p_overwrite);

            } else if (bucket_depth == table_depth) { // false - resize Bucket

                HashMap.Result result = new HashMap.Result();
                result.m_resized = true;

                return result;

            } else
                throw new RuntimeException();
        }
    }

    /**
     * Allocates a ChunkID and calls the bucketsplit from {@link de.hhu.bsinfo.dxram.datastructure.Bucket}. It will send
     * the raw format  over the network if its necessary. Returns a result object based on the executed operation and its
     * result.
     *
     * @param p_address        address where the bucket is stored
     * @param p_bucketCID      ChunkID of the bucket
     * @param p_hash           hashed key
     * @param p_hashFunctionId id for the hash algorithm which should be used
     * @param p_key            key with which the specified value is to be associated
     * @param p_value          value to be associated with the specified key
     * @return a result object based on the executed operation and its result.
     * @throws java.lang.NullPointerException
     * @throws java.lang.RuntimeException
     */
    private HashMap.Result splitBucketAndPut(final long p_address, final long p_bucketCID, final byte[] p_hash,
                                             final byte p_hashFunctionId, final byte[] p_key, final byte[] p_value,
                                             final boolean p_overwrite) {
        HashMap.Result result = new HashMap.Result();

        long cid = allocateCID(m_memory.size().size(p_bucketCID));

        if (m_service.isLocal(cid)) { // new bucket is local

            result = splitBucketAndPutLocalMemory(m_memory, p_address, pin(cid), p_bucketCID, cid, p_hash, p_hashFunctionId, p_key, p_value, p_overwrite);

        } else { // new bucket is global

            Bucket.RawData rawData = Bucket.splitBucket(m_memory, p_address, p_hashFunctionId);

            if (rawData == null)
                throw new RuntimeException();

            if (!ExtendibleHashing.compareBitForDepth(p_hash, Bucket.getDepth(m_reader, p_address))) {

                if (!Bucket.isFull(m_reader, p_address))
                    result = savePut(m_memory, p_address, p_bucketCID, p_key, p_value, p_overwrite);

            } else {

                if (rawData.isEmpty()) {

                    rawData.appendKey((short) p_key.length, p_key);
                    rawData.appendValue((short) p_value.length, p_value);
                    result.m_success = true;

                }
            }

            rawData.finish();
            if (rawData.getByteArray() == null)
                throw new NullPointerException();

            WriteBucketRawDataRequest request = new WriteBucketRawDataRequest(ChunkID.getCreatorID(cid), cid, rawData.getByteArray());
            SignalResponse response = (SignalResponse) m_service.sendSync(request, -1);

            if (!response.wasSuccessful())
                throw new RuntimeException();
        }

        result.m_new_bucket = cid;

        return result;
    }

    /**
     * Returns the allocated ChunkID. The ChunkID can belongs to this peer or a remote peer, based on the NodePool.
     *
     * @param p_bucketSize size of the ChunkID which will be allocated (Bytes)
     * @return the allocated ChunkID
     * @see de.hhu.bsinfo.dxram.datastructure.NodePool
     */
    private long allocateCID(final int p_bucketSize) {
        long address = m_pinning.translate(m_nodepool_cid);
        short nodeId = NodePool.getRandomNode(m_reader, address);

        if (m_service.isLocal(nodeId))
            return m_memory.create().create(p_bucketSize);
        else {

            AllocateChunkRequest request = new AllocateChunkRequest(nodeId, p_bucketSize);
            AllocateChunkResponse response = (AllocateChunkResponse) m_service.sendSync(request, -1);

            return response.getBucketCID();
        }
    }


    /**
     * Returns a result object based on the executed operation and its result. It calls
     * {@link de.hhu.bsinfo.dxram.datastructure.Bucket#put(DXMem, long, byte[], byte[])}.
     *
     * @param p_memory    an instance of DXMem to get direct memory access
     * @param p_address   where the bucket is stored
     * @param p_bucketCID ChunkID of the bucket
     * @param p_key       key with which the specified value is to be associated
     * @param p_value     value to be associated with the specified key
     * @return a result object based on the executed operation and its result.
     * @throws java.lang.RuntimeException
     */
    private static HashMap.Result savePut(@NotNull final DXMem p_memory, long p_address, final long p_bucketCID,
                                          final byte[] p_key, final byte[] p_value, final boolean p_overwrite) {
        HashMap.Result result = new HashMap.Result();

        if (!Bucket.isEnoughSpace(p_memory, p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value))) { // resize Bucket and don't forget unpin, pin and update address

            int new_size = Bucket.sizeForFit(p_memory.rawRead(), p_memory.size(), p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value));

            p_memory.resize().resize(p_bucketCID, new_size);

            p_address = p_memory.pinning().translate(p_bucketCID);
        }

        if (p_overwrite) {

            if (Bucket.contains(p_memory, p_address, p_key)) { // overwrite value by remove this entry and recall ths function

                result.m_overwrite = true;
                Bucket.remove(p_memory, p_address, p_key);

                if (!savePut(p_memory, p_address, p_bucketCID, p_key, p_value, true).m_success) // should never be false (m_success)
                    throw new RuntimeException();

            }
        }

        Bucket.put(p_memory, p_address, p_key, p_value);

        result.m_success = true;


//        log.debug("Save Put -->\nBucket after put the key = " + Arrays.toString(p_key) + "\nand value = "
//                + Arrays.toString(p_value) + "\n" +
//                Bucket.toString(p_memory.size(), p_memory.rawRead(), p_bucketCID, p_address) + "\n");

        return result;
    }

    /**
     * Returns a result object based on the executed operation and its result. The method will split the bucket but
     * will only be called from a request. So the bucket split is local.
     *
     * @param p_memory         an instance of DXMem to get direct memory access
     * @param p_address        address where the bucket is stored
     * @param p_address2       address where the new bucket is stored
     * @param p_bucketCID      ChunkID of the bucket
     * @param p_newBucketCID   ChunkID of the new bucket
     * @param p_hash           hashed key
     * @param p_hashFunctionId id for the hash algorithm which should be used
     * @param p_key            key with which the specified value is to be associated
     * @param p_value          value to be associated with the specified key
     * @return a result object based on the executed operation and its result
     * @throws java.lang.RuntimeException
     */
    private static HashMap.Result splitBucketAndPutLocalMemory(@NotNull final DXMem p_memory, final long p_address, final long p_address2,
                                                               final long p_bucketCID, final long p_newBucketCID,
                                                               final byte[] p_hash, final byte p_hashFunctionId,
                                                               final byte[] p_key, final byte[] p_value,
                                                               final boolean p_overwrite) {
        Bucket.splitBucket(p_memory, p_address, p_address2, p_hashFunctionId);

        if (ExtendibleHashing.compareBitForDepth(p_hash, Bucket.getDepth(p_memory.rawRead(), p_address))) { // try put, true --> new bucket else old bucket

            if (!Bucket.isFull(p_memory.rawRead(), p_address2))
                return savePut(p_memory, p_address2, p_newBucketCID, p_key, p_value, p_overwrite);

        } else {

            if (!Bucket.isFull(p_memory.rawRead(), p_address))
                return savePut(p_memory, p_address, p_bucketCID, p_key, p_value, p_overwrite);
        }

        return new HashMap.Result(); // As caller add the bucket cid to result
    }


    /**
     * Handles a put request from another peer and returns a response for it. Checks for resize as fast as possible.
     *
     * @param p_request request for put operation
     * @param p_memory  an instance of DXMem to get direct memory access
     * @return a response for the put request.
     * @see de.hhu.bsinfo.dxram.datastructure.messages.PutRequest
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    @NotNull
    @Contract("_, _ -> new")
    static PutResponse handlePutRequest(@NotNull final PutRequest p_request, final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_PUT_REQ;

        long address = p_memory.pinning().translate(p_request.getCid());
        boolean isFull = Bucket.isFull(p_memory.rawRead(), address);
        short bucket_depth = Bucket.getDepth(p_memory.rawRead(), address);

        if (isFull && bucket_depth == p_request.getTableDepth()) { // check for resize call as fast as possible

            return new PutResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_RESIZE, ChunkID.INVALID_ID);
        }

        HashMap.Result result = putGlobal(p_memory, p_request, address, isFull, bucket_depth);

        return new PutResponse(p_request, result.getSubSubType(), result.m_new_bucket);
    }

    /**
     * Returns a result object based on the executed operation and its result. The method will decide if a normal put
     * or a bucketsplit will called.
     *
     * @param p_memory  an instance of DXMem to get direct memory access
     * @param p_address wehere the bucket is stored
     * @param p_isFull  indicates if the bucket stores the maximum number of key-value pairs
     * @param p_depth   depth of the bucket
     * @param p_request PutRequest with holds all information
     * @return Returns a result object based on the executed operation and its result
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    private static HashMap.Result putGlobal(final DXMem p_memory, final PutRequest p_request, final long p_address, final boolean p_isFull,
                                            final short p_depth) {
        if (!p_isFull) {

            return savePut(p_memory, p_address, p_request.getCid(), p_request.getKey(), p_request.getValue(), p_request.getOverwrite());

        } else if (p_depth < p_request.getTableDepth()) { // bucket split

            long cid2 = p_memory.create().create(p_memory.size().size(p_request.getCid()));
            long address2 = pin(p_memory, cid2);

            HashMap.Result result = splitBucketAndPutLocalMemory(p_memory, p_address, address2, p_request.getCid(), cid2,
                    p_request.getHashedKey(), p_request.getHashFunctionId(), p_request.getKey(), p_request.getValue(),
                    p_request.getOverwrite());
            result.m_new_bucket = cid2;
            return result;

        } else
            throw new RuntimeException();
    }

    /**
     * Handles a request for writing the raw data of a bucket into a bucket on this peer. It returns a response always
     * with success as signal.
     *
     * @param p_request request for writing the raw data of a bucket into a bucket on this peer.
     * @param p_memory  an instance of DXMem to get direct memory access
     * @return a response always with success as signal
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    @NotNull
    @Contract("_, _ -> new")
    static SignalResponse handleWriteBucketRequest(@NotNull final WriteBucketRawDataRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_WRITE_BUCKET_REQ;

        Bucket.initialize(p_memory.rawWrite(), p_memory.pinning().translate(p_request.getCid()), p_request.getRawData());

        return new SignalResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_SUCCESS);
    }

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param p_key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     * @throws java.lang.NullPointerException
     * @see #put(java.lang.Object, java.lang.Object)
     */
    public V get(final K p_key) {
        assert p_key != null;
        V value;
        byte[] key, valueBytes;

        if (m_serializeKey) // Serialize
            key = Skema.serialize(p_key);
        else
            key = (byte[]) p_key;

        final byte[] hash = HashFunctions.hash(m_hashFunctionId, key); // Hash key

        m_lock.readLock().lock(); // lock reentrant write lock

        final long cid = Hashtable.lookup(m_reader, m_hashtableAdr, ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtableAdr))); // Lookup

        if (m_service.isLocal(cid)) { // bucket is local

            valueBytes = getFromLocalMemory(m_memory, cid, key);

        } else { // bucket is global

            GetRequest request = new GetRequest(ChunkID.getCreatorID(cid), key, cid);

            GetResponse response = (GetResponse) m_service.sendSync(request, -1);

            valueBytes = response.getValue();
        }

        m_lock.readLock().unlock(); // lock reentrant write lock

        if (valueBytes == null) // key was not found
            return null;

        if (m_serializeValue) { // deserialize

            value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaDataAdr));
            Skema.deserialize(value, valueBytes);

        } else
            value = (V) valueBytes;

        return value;
    }


    /**
     * Returns the value to the matching key. from local memory.
     *
     * @param p_memory an instance of DXMem to get direct memory access
     * @param p_cid    ChunkID of the bucket where the k-v pair should be stored
     * @param p_key    the key whose associated value is to be returned
     * @return the value to the matching key
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    private static byte[] getFromLocalMemory(@NotNull final DXMem p_memory, final long p_cid, final byte[] p_key) {
        return Bucket.get(p_memory, p_memory.pinning().translate(p_cid), p_key);
    }


    /**
     * Handles a request for a get operation and returns a response for it.
     *
     * @param p_request request for getting the value whose associated to the key
     * @param p_memory  an instance of DXMem to get direct memory access
     * @return a GetResponse with the associated value or null.
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    @NotNull
    @Contract("_, _ -> new")
    static GetResponse handleGetRequest(final GetRequest p_request, final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_GET_REQ;

        return new GetResponse(p_request, getFromLocalMemory(p_memory, p_request.getCid(), p_request.getKey()));
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param p_key key whose mapping is to be removed from the map
     * @return the previous value associated with key, or null if there was no mapping for key.
     */
    public V remove(final K p_key) {
        assert p_key != null;
        V value;
        byte[] key, valueBytes;

        if (m_serializeKey) // Serialize
            key = Skema.serialize(p_key);
        else
            key = (byte[]) p_key;

        final byte[] hash = HashFunctions.hash(m_hashFunctionId, key); // Hash key

        m_lock.writeLock().lock(); // lock reentrant write lock

        final long cid = Hashtable.lookup(m_reader, m_hashtableAdr, ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtableAdr))); // Lookup

        log.debug("Remove is : " + (m_service.isLocal(cid) ? "local" : "global") + "\n");

        if (m_service.isLocal(cid)) { // bucket is local

            valueBytes = Bucket.remove(m_memory, m_pinning.translate(cid), key);

        } else { // bucket is global

            RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(cid), key, cid);

            RemoveResponse response = (RemoveResponse) m_service.sendSync(request, -1);

            valueBytes = response.getValue();
        }

        Metadata.decrementSize(m_writer, m_reader, m_metaDataAdr); // Metadata decrement size

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        if (valueBytes == null)
            return null;

        if (m_serializeValue) { // deserialize

            value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaDataAdr));
            Skema.deserialize(value, valueBytes);

        } else
            value = (V) valueBytes;

        return value;
    }

    /**
     * Removes the entry for the specified key only if it is currently mapped to the specified value.
     *
     * @param p_key   key whose mapping is to be removed from the map
     * @param p_value value expected to be associated with the specified key
     * @return true if the value was removed
     */
    public boolean remove(final K p_key, final V p_value) {
        assert p_key != null && p_value != null;
        byte[] key, value;
        boolean result;

        if (m_serializeKey) // Serialize
            key = Skema.serialize(p_key);
        else
            key = (byte[]) p_key;

        if (m_serializeValue)
            value = Skema.serialize(p_value);
        else
            value = (byte[]) p_value;

        final byte[] hash = HashFunctions.hash(m_hashFunctionId, key); // Hash key

        m_lock.writeLock().lock(); // lock reentrant write lock

        final long cid = Hashtable.lookup(m_reader, m_hashtableAdr, ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtableAdr))); // Lookup

        log.debug("Remove is : " + (m_service.isLocal(cid) ? "local" : "global") + "\n");

        if (m_service.isLocal(cid)) { // bucket is local

            result = Bucket.remove(m_memory, m_pinning.translate(cid), key, value);

        } else { // bucket is global

            RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(cid), key, value, cid);

            SignalResponse response = (SignalResponse) m_service.sendSync(request, -1);

            result = response.wasSuccessful();
        }

        if (result)  // Metadata decrement size
            Metadata.decrementSize(m_writer, m_reader, m_metaDataAdr);

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        return result;
    }

    /**
     * Handles a request for a remove operation and returns a response with the matching key.
     *
     * @param p_request request for a remove operation without the value
     * @param p_memory  an instance of DXMem to get direct memory access
     * @return a response with the matching key.
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    static RemoveResponse handleRemoveRequest(@NotNull final RemoveRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_REQ;

        byte[] value = Bucket.remove(p_memory, p_memory.pinning().translate(p_request.getCid()), p_request.getKey());
        if (value == null)
            throw new NullPointerException();

        return new RemoveResponse(p_request, value);
    }

    /**
     * Handles a request for a remove operation and returns a response with the matching key-value pair.
     *
     * @param p_request request for a remove operation with key
     * @param p_memory  an instance of DXMem to get direct memory access
     * @return a response with the matching key-value pair.
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    static SignalResponse handleRemoveWithKeyRequest(@NotNull final RemoveRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ;

        boolean result = Bucket.remove(p_memory, p_memory.pinning().translate(p_request.getCid()), p_request.getKey(), p_request.getValue());

        return new SignalResponse(p_request,
                (result ? DataStructureMessageTypes.SUBSUBTYPE_SUCCESS : DataStructureMessageTypes.SUBSUBTYPE_ERROR));
    }


    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map.
     */
    public synchronized long size() { // TODO: Synchronize with write lock?
        return Metadata.getHashMapSize(m_reader, m_metaDataAdr);
    }

    /**
     * Returns true if this map contains no key-value mappings.
     *
     * @return true if this map contains no key-value mappings
     */
    public synchronized boolean isEmpty() { // TODO: Synchronize with write lock?
        return Metadata.getHashMapSize(m_reader, m_metaDataAdr) == 0;
    }


    /**
     * Removes all of the mappings from this map. The map will be empty after this call returns.
     * The depth of the Hashtable will not be reset
     */
    public void clear() {
        clear(true);
    }

    /**
     * Removes all of the mappings from this map. The map will be empty after this call returns.
     * The Hashtable and buckets will not be removed.
     *
     * @param p_withInitializer indicates if the HashMap will be initialized for another usage or not
     */
    private void clear(final boolean p_withInitializer) {
        m_lock.writeLock().lock(); // lock reentrant write lock

        clearAllBuckets();// remove all Buckets

        if (p_withInitializer) {

            long bucketCID = m_memory.create().create(Metadata.getIndividualBucketSize(m_reader, pin(m_metaDataCID)));
            long address = pin(bucketCID);
            m_memory.pinning().unpinCID(m_metaDataCID);
            Bucket.initialize(m_writer, address, (short) 0);

            clearHashtable(bucketCID); // set Hashtable to new Bucket CID

        }

        Metadata.clearSize(m_writer, m_metaDataAdr); // reset size

        m_lock.writeLock().unlock(); // unlock reentrant write lock
    }

    /**
     * Removes all Buckets. All different ChunkIDs from the Hashtable are collected and grouped by their NodeID.
     * For every NodeID a asynchronous message will be send with all ChunkIDs for this NodeID.
     * {@link #handleGetRequest(GetRequest, DXMem)} will remove the ChunkIDs from the messages.
     */
    private void clearAllBuckets() {
        HashSet<Long> cids = Hashtable.bucketCIDs(m_memory.size(), m_reader, m_hashtableCID, m_hashtableAdr); // differen ChunkIDs

        java.util.HashMap<Short, ArrayList<Long>> grouped_cids = getAllGroupedChunkIDs();

        for (short nodeId : grouped_cids.keySet()) { // iterate over all NodeIDs

            if (m_service.isLocal(nodeId)) { // local remove

                grouped_cids.get(nodeId).forEach((cid) -> {
                    m_pinning.unpinCID(cid);
                    m_memory.remove().remove(cid, false);
                });

            } else { // global remove

                m_service.sendAsync(new ClearMessage(nodeId, Longs.toArray(grouped_cids.get(nodeId))));
            }
        }
    }

    /**
     * Clears the Hashtable by calling {@link de.hhu.bsinfo.dxram.datastructure.Hashtable#clear(Size, RawWrite, long, long, long)}.
     *
     * @param p_defaultEntry
     * @see de.hhu.bsinfo.dxram.datastructure.Hashtable#clear(Size, RawWrite, long, long, long)
     */
    private void clearHashtable(final long p_defaultEntry) {
        Hashtable.clear(m_memory.size(), m_writer, m_hashtableCID, m_hashtableAdr, p_defaultEntry);
    }

    /**
     * Removes all bucket from the memory.
     *
     * @param p_memory an instance of DXMem to get direct memory access
     * @param p_arr    array with ChunkIDs
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    private static void clearAllBucketsFromLocalMemory(final DXMem p_memory, @NotNull final long[] p_arr) {
        for (long cid : p_arr) {
            p_memory.pinning().unpinCID(cid);
            p_memory.remove().remove(cid);
        }
    }


    /**
     * Handles a request for removes ChunkIDs from the local memory.
     *
     * @param p_message message which indicates a remove of ChunkIDs from the local memory
     * @param p_memory  an instance of DXMem to get direct memory access
     * @see de.hhu.bsinfo.dxmem.DXMem
     */
    static void handleClearRequest(@NotNull final ClearMessage p_message, final DXMem p_memory) {
        assert p_message.getSubtype() == DataStructureMessageTypes.SUBTYPE_CLEAR_MESSAGE;

        clearAllBucketsFromLocalMemory(p_memory, p_message.getCids());
    }

    /**
     * Returns the value to which the specified key is mapped, or defaultValue if this map contains no mapping for the key.
     *
     * @param p_key          the key whose associated value is to be returned
     * @param p_defaultValue the default mapping of the key
     * @return the value to which the specified key is mapped, or defaultValue if this map contains no mapping for the key
     */
    public V getOrDefault(final K p_key, final V p_defaultValue) {
        V ret = this.get(p_key);
        return ret == null ? p_defaultValue : ret;
    }

    /**
     * Writes all memory information from this object into the file.
     *
     * @param p_file File where the data is written to
     */
    void extractMemoryInformation(final File p_file) {
        long metadata;
        long allocated = 0;
        long used = 0;
        long numberOfBuckets = 0;

        log.debug("Get all grouped and different ChunkIDs/Buckets");
        java.util.HashMap<Short, ArrayList<Long>> grouped_cids = getAllGroupedChunkIDs();

        log.debug("Start Extraction from Memory");
        for (short nodeId : grouped_cids.keySet()) { // iterate over all NodeIDs

            long[] group = Longs.toArray(grouped_cids.get(nodeId));
            numberOfBuckets += group.length;

            if (m_service.isLocal(nodeId)) { // local

                long[] info = extractMemoryInformationFromLocalMemory(m_memory, group);
                allocated += info[0];
                used += info[1];

            } else { // global

                MemoryInformationResponse response = (MemoryInformationResponse) m_service.sendSync(new MemoryInformationRequest(nodeId, group), -1);
                allocated += response.getAllocated();
                used += response.getUsed();

            }
        }

        metadata = numberOfBuckets * Bucket.getMetadataMemSize();

        log.debug("Write information to file: " + p_file.getAbsolutePath());
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(p_file))) {

            bw.write(String.format("%d\n", this.size()));
            bw.write(String.format("%d,%d,%d,%d\n", numberOfBuckets, allocated, used, metadata));
            short depth = Hashtable.getDepth(m_reader, m_hashtableAdr);
            bw.write(String.format("%d,%d\n", depth, m_memory.size().size(m_hashtableCID)));
            bw.write(String.format("%d\n", m_memory.size().size(m_metaDataCID)));
            bw.write(String.format("%d\n", m_memory.size().size(m_nodepool_cid)));

        } catch (IOException p_e) {
            log.error("IOException catched while try to write memory information to " + p_file.getAbsolutePath());
        }
    }

    static MemoryInformationResponse handleMemoryInformationRequest(final MemoryInformationRequest p_request, final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_MEM_INFO_REQ;

        long[] information = extractMemoryInformationFromLocalMemory(p_memory, p_request.getCids());

        return new MemoryInformationResponse(p_request, information[0], information[1]);
    }

    private static long[] extractMemoryInformationFromLocalMemory(final DXMem p_memory, final long[] p_chunkID) {
        long[] information = {0L, 0L};

        for (long cid : p_chunkID) {
            information[0] += p_memory.size().size(cid);
            information[1] += Bucket.getUsedBytes(p_memory.rawRead(), p_memory.pinning().translate(cid), false);
        }

        return information;
    }

    /**
     * Collects and return all different ChunkIDs from the hashtable and groups them by their NodeID.
     *
     * @return all different ChunkIDs from the hashtable and groups them by their NodeID.
     */
    private java.util.HashMap<Short, ArrayList<Long>> getAllGroupedChunkIDs() {
        HashSet<Long> cids = Hashtable.bucketCIDs(m_memory.size(), m_reader, m_hashtableCID, m_hashtableAdr); // differen ChunkIDs

        java.util.HashMap<Short, ArrayList<Long>> grouped_cids = new java.util.HashMap<>(cids.size());

        cids.forEach((cid) -> { // group_cids
            short nodeId = ChunkID.getCreatorID(cid);
            if (grouped_cids.containsKey(nodeId)) { // exist --> add to list
                grouped_cids.get(nodeId).add(cid);
            } else { // not exist --> put Entry<nodeId,list>
                ArrayList<Long> list = new ArrayList<>();
                list.add(cid);
                grouped_cids.put(nodeId, list);
            }
        });

        return grouped_cids;
    }

    /**
     * Checks the initial parameter for the constructor of this HashMap. Returns true if all parameters are proper.
     *
     * @param p_name            identifier for the metadata chunk
     * @param p_initialCapacity minimum capacity of this instance to store key-value pairs
     * @param p_onlinePeers     list of all online peers (no superpeers) in dxram
     * @param p_numberOfNodes   number of peers, where the HashMap should be distributed their buckets
     * @param p_keyBytes        size of a key
     * @param p_valueBytes      size of a value
     * @param p_hashFunctionId  id for the hash algorithm which should be used
     * @return true if all parameters are proper.
     */
    static boolean assertInitialParameter(final String p_name, final int p_initialCapacity, final List<Short> p_onlinePeers,
                                          final int p_numberOfNodes, final short p_keyBytes, final short p_valueBytes,
                                          final byte p_hashFunctionId) {
        return p_initialCapacity > 1 && (p_numberOfNodes > 0 || p_numberOfNodes == -1) && p_keyBytes > 0 &&
                p_valueBytes > 0 && HashFunctions.isProperValue(p_hashFunctionId) && p_onlinePeers.size() > 0 &&
                NameserviceComponent.hasCorrectNameFormat(p_name);
    }


    /**
     * Returns the calculated depth for the Hashtable based on a value which represents the initial capacity
     * of this HashMap.
     *
     * @param p_value the initial capacity of this HashMap.
     * @return the calculated depth for the Hashtable
     */
    @Contract(pure = true)
    private short calcTableDepth(int p_value) {
        assert p_value >= 0;

        if (p_value == 0)
            return 1;

        int highestExponent = HASH_TABLE_DEPTH_LIMIT + 1; // + 1 generates space

        do {
            p_value = p_value << 1;
            highestExponent--;
        } while (p_value > 0);

        short diff = (short) (highestExponent - BUCKET_ENTRIES_EXP);

        return diff > 0 ? diff : 1;

    }

}
package de.hhu.bsinfo.dxram.datastructure;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.primitives.Longs;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.core.MemoryRuntimeException;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxram.datastructure.messages.*;
import de.hhu.bsinfo.dxram.datastructure.util.*;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.skema.Skema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * @param <K> Generic type for Key
 * @param <V> Generic type for Value
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
        BUCKET_ENTRIES_EXP = 2;
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

    private final long m_metaData_cid; // should be recovered
    private final long m_metaData_address; // should be recovered

    private final long m_hashtable_cid; // should be recovered
    private long m_hashtable_address; // should be recovered

    private final long m_nodepool_cid; // should be recovered

    private final byte m_hashFunctionId; // should be recovered

    private boolean m_skemaRegistrationSwitch;
    private boolean m_serializeKey;
    private boolean m_serializeValue;


    HashMap(final DXMem p_memory, final DataStructureService p_service, final String p_name, final int p_initialCapacity,
            final List<Short> p_onlinePeers, final int p_numberOfNodes, final short p_keyBytes,
            final short p_valueBytes, final byte p_hashFunctionId) {
        m_lock = new ReentrantReadWriteLock(true);

        m_service = p_service;
        m_memory = p_memory;
        m_reader = m_memory.rawRead();
        m_writer = m_memory.rawWrite();

        m_hashFunctionId = p_hashFunctionId;
        m_skemaRegistrationSwitch = true;
        m_serializeKey = true;
        m_serializeValue = true;

        short hashtable_depth = calcTableDepth(p_initialCapacity); // calculate hashtable_depth

        m_nodepool_cid = initNodePool(p_onlinePeers, p_numberOfNodes); // Init NodePool

        long bucket_cid = initBucket(BUCKET_INITIAL_DEPTH, Bucket.calcIndividualBucketSize(HashMap.BUCKET_ENTRIES, p_keyBytes, p_valueBytes)); // Init Bucket

        m_hashtable_cid = initHashtable(hashtable_depth, bucket_cid); // Init Hashtable
        m_hashtable_address = m_memory.pinning().pin(m_hashtable_cid).getAddress();

        m_metaData_cid = initMetaData(m_nodepool_cid, m_hashtable_cid, Bucket.calcIndividualBucketSize(BUCKET_ENTRIES, p_keyBytes, p_valueBytes), p_hashFunctionId); // Init Metadata
        m_metaData_address = m_memory.pinning().pin(m_metaData_cid).getAddress();

        m_service.registerDataStructure(m_metaData_cid, p_name); // Register Metadata
    }

    void removeThisObject() {
        m_lock.writeLock().lock(); // lock reentrant write lock7

        this.clear(false);

        m_memory.remove().remove(m_nodepool_cid);

        m_memory.pinning().unpinCID(m_hashtable_address);
        m_memory.remove().remove(m_hashtable_cid);

        m_memory.pinning().unpinCID(m_metaData_address);

        m_memory.remove().remove(m_metaData_cid);

        m_lock.writeLock().unlock(); // unlock reentrant write lock
    }

    private long initNodePool(@NotNull final List<Short> p_onlinePeers, final int p_numberOfNodes) {
        long cid = m_memory.create().create(NodePool.getInitialMemorySize(p_onlinePeers.size(), p_numberOfNodes));
        long address = lockAndPin(cid);

        NodePool.initialize(m_writer, p_onlinePeers, address, p_numberOfNodes);

        unlockAndUnpin(cid);

        return cid;
    }

    private long initBucket(final short p_depth, final int p_individualBucketSize) {
        int size = Bucket.getInitialMemorySize() + p_individualBucketSize;
        long cid = m_memory.create().create(size);
        long address = lockAndPin(cid);

        Bucket.initialize(m_writer, address, p_depth);

        unlockAndUnpin(cid);

        return cid;
    }

    private long initHashtable(final short p_depth, final long p_defaultEntry) {
        int initialSize = Hashtable.getInitialMemorySize(p_depth);
        long cid = m_memory.create().create(initialSize);
        long address = lockAndPin(cid);

        Hashtable.initialize(m_writer, address, initialSize, p_depth, p_defaultEntry);

        unlockAndUnpin(cid);

        return cid;
    }

    private long initMetaData(final long p_nodePool_cid, final long p_hashtable_cid, final int p_individual_bucketSize,
                              final byte p_hashFunctionId) {
        long cid = m_memory.create().create(Metadata.getInitialMemorySize());
        long address = lockAndPin(cid);

        Metadata.initialize(m_writer, address, p_hashtable_cid, p_nodePool_cid, 0L, SKEMA_DEFAULT_ID, p_individual_bucketSize, p_hashFunctionId);

        unlockAndUnpin(cid);

        return cid;
    }


    /*** Synchronize ***/
    /**
     * pinned a chunkId and will lock it after. But it's not synchronized.
     * So don't use it if u will resize after your lock. It will returned a wrong address for another thread.
     *
     * @param p_cid
     * @return
     */
    private long lockAndPin(final long p_cid) {
        long address = pin(p_cid);

        m_memory.lock().lock(p_cid, true, -1);

        return address;
    }

    /**
     * unlock a chunkId and will unpin it after. But it's not synchronized.
     * You can unpin an address which is actually used by another thread and if this thread try to unpin it will failed.
     *
     * @param p_cid
     */
    private void unlockAndUnpin(final long p_cid) {

        m_memory.lock().unlock(p_cid, true);

        m_memory.pinning().unpinCID(p_cid);
    }

    private long pin(final long p_cid) {
        return pin(m_memory, p_cid);
    }

    private static long pin(@NotNull final DXMem p_memory, final long p_cid) {
        Pinning.PinnedMemory pinnedMemory = p_memory.pinning().pin(p_cid);

        if (!pinnedMemory.isStateOk()) {
            log.error("CID = " + p_cid + " could not be pinned. State = " + pinnedMemory.getState().toString());
            throw new RuntimeException();
        }

        return pinnedMemory.getAddress();
    }

    public boolean put(final K p_key, final V p_value) {
        assert p_key != null && p_value != null;
        byte[] key, value;

        if (m_skemaRegistrationSwitch) { // Skema registration
            if (p_key.getClass() == byte[].class)
                m_serializeKey = false;
            if (p_value.getClass() == byte[].class)
                m_serializeValue = false;
            else
                Metadata.setSkemaValueType(m_writer, m_metaData_address, Skema.resolveIdentifier(p_value.getClass()));

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

        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index); // Lookup in Hashtable

        return putLoop(cid, index, hash, key, value);
    }

    private static class Result {

        private boolean m_success = false;
        private long m_new_bucket = ChunkID.INVALID_ID;
        private boolean m_resized = false;
        private boolean m_error = false;
        private boolean m_overwrite = false;

        /**
         * Only used if u checked SubSubtype Busy you can use this method to get the correct subsubtype from this Object.
         * It should only be used for handle a request.
         *
         * @return
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

    private boolean putLoop(long p_cid, int p_index, final byte[] p_hash, final byte[] p_key, final byte[] p_value) {
        HashMap.Result result;
        do {

            if (m_service.isLocal(p_cid)) {

                long address = m_memory.pinning().pin(p_cid).getAddress();

                result = putLocal(address, p_cid, p_hash, p_key, p_value);

                m_memory.pinning().unpinCID(p_cid);

            } else {

                PutRequest request = new PutRequest(ChunkID.getCreatorID(p_cid), p_key, p_value, Hashtable.getDepth(m_reader, m_hashtable_address), p_cid, m_hashFunctionId);
                PutResponse response = (PutResponse) m_service.sendSync(request, -1);
                result = consumePutResponse(response);

                if (result.m_error)
                    log.error("Put Request throws an error");

            }

            // handle optional resize
            if (result.m_resized) {

                short depth = Hashtable.getDepth(m_reader, m_hashtable_address);

                if (depth == MAX_DEPTH) {

                    m_lock.writeLock().unlock();

                    log.error("Hashtable reached maximum depth of " + MAX_DEPTH);

                    return false;

                } else if (m_memory.stats().getHeapStatus().getTotalSizeBytes() < ((long) Math.pow(2, depth + 1) * Long.BYTES + 2))
                    throw new MemoryRuntimeException("Total Bytes: " + m_memory.stats().getHeapStatus().getTotalSizeBytes() + " But want to allocate more");
                else {

                    m_hashtable_address = Hashtable.resize(m_reader, m_writer, m_memory.resize(), m_memory.pinning(), m_hashtable_cid, m_hashtable_address);
                    p_index = ExtendibleHashing.extendibleHashing(p_hash, Hashtable.getDepth(m_reader, m_hashtable_address));
                    p_cid = Hashtable.lookup(m_reader, m_hashtable_address, p_index);

                }
            }

            if (result.m_new_bucket != ChunkID.INVALID_ID) {  // handle optional bucket split

                Hashtable.splitForEntry(m_reader, m_writer, m_memory.size(), m_hashtable_cid, m_hashtable_address, p_cid, p_index, result.m_new_bucket);
                p_cid = Hashtable.lookup(m_reader, m_hashtable_address, p_index);

            }

            if (result.m_error) { // handle error

                m_lock.writeLock().unlock();

                log.error("Could not put key into HashMap\nKey = " + Arrays.toString(p_key));

                return false;

            }

            if (result.m_success && !result.m_overwrite) // Metadata increment size
                Metadata.incrementSize(m_writer, m_reader, m_metaData_address);


        } while (!result.m_success);

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        return true;
    }

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

    private HashMap.Result putLocal(final long p_address, final long p_bucketCID, final byte[] p_hash, final byte[] p_key, final byte[] p_value) {
        if (!Bucket.isFull(m_reader, p_address)) { // Bucket has space

            HashMap.Result result = savePut(m_memory, p_address, p_bucketCID, p_key, p_value);

            if (!result.m_success)
                log.warn("SavePut failed for key: " + Arrays.toString(p_key));

            return result;

        } else { // Bucket has maximum entries

            short bucket_depth = Bucket.getDepth(m_reader, p_address);

            int table_depth = Hashtable.getDepth(m_reader, m_hashtable_address);

            if (bucket_depth < table_depth) { // true - split bucket

                return splitBucketAndPut(p_address, p_bucketCID, p_hash, m_hashFunctionId, p_key, p_value);

            } else if (bucket_depth == table_depth) { // false - resize Bucket

                HashMap.Result result = new HashMap.Result();
                result.m_resized = true;

                return result;

            } else
                throw new RuntimeException();

        }
    }

    private HashMap.Result splitBucketAndPut(final long p_address, final long p_bucketCID, final byte[] p_hash, final byte p_hashFunctionId, final byte[] p_key, final byte[] p_value) {
        long cid = allocateCID(m_memory.size().size(p_bucketCID));

        HashMap.Result result = new HashMap.Result();

        if (m_service.isLocal(cid)) { // new bucket is local

            long address2 = pin(cid);
            result = splitBucketAndPutLocalMemory(m_memory, p_address, address2, p_bucketCID, cid, p_hash, p_hashFunctionId, p_key, p_value);
            m_memory.pinning().unpinCID(cid);

        } else { // new bucket is global

            Bucket.RawData rawData = Bucket.splitBucket(m_reader, m_writer, p_address, p_hashFunctionId);

            if (rawData == null)
                throw new RuntimeException();

            if (!ExtendibleHashing.compareBitForDepth(p_hash, Bucket.getDepth(m_reader, p_address))) {

                if (!Bucket.isFull(m_reader, p_address))
                    result = savePut(m_memory, p_address, p_bucketCID, p_key, p_value);

            } else {

                if (rawData.isEmpty()) {

                    rawData.appendKey((short) p_key.length, p_key);
                    rawData.appendValue((short) p_value.length, p_value);
                    result.m_success = true;

                }
            }

            if (rawData.getByteArray() == null) {

                throw new NullPointerException();

            }

            WriteBucketRawDataRequest request = new WriteBucketRawDataRequest(ChunkID.getCreatorID(cid), cid, rawData.getByteArray());
            SignalResponse response = (SignalResponse) m_service.sendSync(request, -1);

            if (!response.wasSuccessful())
                throw new RuntimeException();

        }

        result.m_new_bucket = cid;

        return result;
    }

    private long allocateCID(final int p_bucketSize) {
        long address = lockAndPin(m_nodepool_cid);
        short nodeId = NodePool.getRandomNode(m_reader, address);

        unlockAndUnpin(m_nodepool_cid);

        if (m_service.isLocal(nodeId))
            return m_memory.create().create(p_bucketSize);
        else {

            AllocateChunkRequest request = new AllocateChunkRequest(nodeId, p_bucketSize);
            AllocateChunkResponse response = (AllocateChunkResponse) m_service.sendSync(request, -1);

            return response.getBucketCID();
        }
    }


    private static HashMap.Result savePut(@NotNull final DXMem p_memory, long p_address, final long p_bucketCID,
                                          final byte[] p_key, final byte[] p_value) {
        HashMap.Result result = new HashMap.Result();

        if (!Bucket.isEnoughSpace(p_memory.rawRead(), p_memory.size(), p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value))) { // resize Bucket and don't forget unpin, pin and update address

            int new_size = Bucket.sizeForFit(p_memory.rawRead(), p_memory.size(), p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value));

            p_memory.resize().resize(p_bucketCID, new_size);

            p_address = p_memory.pinning().translate(p_bucketCID); // TODO: it could be good to migrate the chunk
        }

        if (Bucket.contains(p_memory.rawRead(), p_address, p_key)) { // overwrite value by remove this entry and recall ths function

            result.m_overwrite = true;
            Bucket.remove(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_key);

            if (!savePut(p_memory, p_address, p_bucketCID, p_key, p_value).m_success) // should never be false (m_success)
                throw new RuntimeException();

        }

        Bucket.put(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_key, p_value);

        result.m_success = true;


//        log.debug("Save Put -->\nBucket after put the key = " + Arrays.toString(p_key) + "\nand value = "
//                + Arrays.toString(p_value) + "\n" +
//                Bucket.toString(p_memory.size(), p_memory.rawRead(), p_bucketCID, p_address) + "\n");

        return result;
    }

    private static HashMap.Result splitBucketAndPutLocalMemory(@NotNull final DXMem p_memory, final long p_address, final long p_address2,
                                                               final long p_bucketCID, final long p_newBucketCID,
                                                               final byte[] p_hash, final byte p_hashFunctionId,
                                                               final byte[] p_key, final byte[] p_value) {
//        log.debug("Bucket Split for Bucket = " + ChunkID.toHexString(p_bucketCID) +
//                "\nnew Bucket = " + ChunkID.toHexString(p_newBucketCID) + "\n");

        Bucket.splitBucket(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_address2, p_hashFunctionId);

        if (ExtendibleHashing.compareBitForDepth(p_hash, Bucket.getDepth(p_memory.rawRead(), p_address))) { // try put, true --> new bucket else old bucket

            if (!Bucket.isFull(p_memory.rawRead(), p_address2))
                return savePut(p_memory, p_address2, p_newBucketCID, p_key, p_value);

        } else {

            if (!Bucket.isFull(p_memory.rawRead(), p_address))
                return savePut(p_memory, p_address, p_bucketCID, p_key, p_value);
        }

//        log.debug("Bucket Split was ok but creates no space for put\n");

        return new HashMap.Result(); // As caller add the bucket cid to result
    }


    @NotNull
    @Contract("_, _ -> new")
    static PutResponse handlePutRequest(@NotNull final PutRequest p_request, final DXMem p_memory) {

        long address = pin(p_memory, p_request.getCid());
        boolean isFull = Bucket.isFull(p_memory.rawRead(), address);
        short bucket_depth = Bucket.getDepth(p_memory.rawRead(), address);

        if (isFull && bucket_depth == p_request.getTableDepth()) { // check for resize call as fast as possible

            p_memory.pinning().unpinCID(p_request.getCid());

            return new PutResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_RESIZE, ChunkID.INVALID_ID);
        }

        HashMap.Result result = putGlobal(p_memory, address, isFull, bucket_depth, p_request.getTableDepth(),
                p_request.getCid(), p_request.getHashedKey(), p_request.getKey(), p_request.getValue(),
                p_request.getHashFunctionId());

        p_memory.pinning().unpinCID(p_request.getCid());

        return new PutResponse(p_request, result.getSubSubType(), result.m_new_bucket);
    }

    private static HashMap.Result putGlobal(final DXMem p_memory, final long p_address, final boolean p_isFull,
                                            final short p_depth, final short p_tableDepth, final long p_cid,
                                            final byte[] p_hash, final byte[] p_key, final byte[] p_value,
                                            final byte p_hashFunctionId) {
        HashMap.Result result;
        if (!p_isFull) {

            result = savePut(p_memory, p_address, p_cid, p_key, p_value);

        } else if (p_depth < p_tableDepth) { // bucket split

            long cid2 = p_memory.create().create(p_memory.size().size(p_cid));
            long address2 = pin(p_memory, cid2);


            result = splitBucketAndPutLocalMemory(p_memory, p_address, address2, p_cid, cid2, p_hash, p_hashFunctionId, p_key, p_value);
            result.m_new_bucket = cid2;

            p_memory.pinning().unpinCID(cid2);

        } else
            throw new RuntimeException();

        return result;
    }

    @NotNull
    @Contract("_, _ -> new")
    static SignalResponse handleWriteBucketRequest(@NotNull final WriteBucketRawDataRequest p_request, @NotNull final DXMem p_memory) {
        Bucket.initialize(p_memory.rawWrite(), pin(p_memory, p_request.getCid()), p_request.getRawData());

        p_memory.pinning().unpinCID(p_request.getCid());

        return new SignalResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_SUCCESS);

    }

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


        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address)); // Lookup in Hashtable
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);

        if (m_service.isLocal(cid)) {

            valueBytes = getFromLocalMemory(m_memory, cid, key);

        } else {

            GetRequest request = new GetRequest(ChunkID.getCreatorID(cid), key, cid);

            GetResponse response = (GetResponse) m_service.sendSync(request, -1);

            if (response.getValue() == null) {

                m_lock.readLock().unlock();

                return null;
            }

            valueBytes = response.getValue();
        }

        m_lock.readLock().unlock(); // lock reentrant write lock

        if (valueBytes == null)
            throw new NullPointerException();

        if (m_serializeValue) { // deserialize

            value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaData_address));
            Skema.deserialize(value, valueBytes);

        } else
            value = (V) valueBytes;

        return value;
    }


    private static byte[] getFromLocalMemory(@NotNull final DXMem p_memory, final long p_cid, final byte[] p_key) {
        long address = p_memory.pinning().pin(p_cid).getAddress();

        byte[] value = Bucket.get(p_memory.rawRead(), address, p_key);

        p_memory.pinning().unpinCID(p_cid);

        return value;
    }


    @NotNull
    @Contract("_, _ -> new")
    static GetResponse handleGetRequest(final GetRequest p_request, final DXMem p_memory) {
        return new GetResponse(p_request, getFromLocalMemory(p_memory, p_request.getCid(), p_request.getKey()));
    }


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

        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);

        log.debug("Remove is : " + (m_service.isLocal(cid) ? "local" : "global") + "\n");

        if (m_service.isLocal(cid)) {

            long address = m_memory.pinning().pin(cid).getAddress();

            valueBytes = Bucket.remove(m_reader, m_writer, address, key);

            m_memory.pinning().unpinCID(cid);

        } else {

            RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(cid), key, cid);

            RemoveResponse response = (RemoveResponse) m_service.sendSync(request, -1);

            valueBytes = response.getValue();

        }

        if (valueBytes == null) {

            m_lock.writeLock().unlock();

            return null;
        }

        Metadata.decrementSize(m_writer, m_reader, m_metaData_address); // Metadata decrement size

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        if (m_serializeValue) { // deserialize

            value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaData_address));
            Skema.deserialize(value, valueBytes);

        } else
            value = (V) valueBytes;

        return value;
    }

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

        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address)); // Lookup in Hashtable
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);


        if (m_service.isLocal(cid)) {

            long address = m_memory.pinning().pin(cid).getAddress();

            result = Bucket.remove(m_reader, m_writer, address, key, value);

            m_memory.pinning().unpinCID(cid);

        } else {

            RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(cid), key, value, cid);

            SignalResponse response = (SignalResponse) m_service.sendSync(request, -1);

            result = response.wasSuccessful();
        }

        if (result)  // Metadata decrement size
            Metadata.decrementSize(m_writer, m_reader, m_metaData_address);

        m_lock.writeLock().unlock(); // unlock reentrant write lock

        return result;
    }


    static RemoveResponse handleRemoveRequest(@NotNull final RemoveRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_REQ;

        long address = pin(p_memory, p_request.getCid());

        byte[] value = Bucket.remove(p_memory.rawRead(), p_memory.rawWrite(), address, p_request.getKey());
        if (value == null)
            throw new NullPointerException();

        RemoveResponse response = new RemoveResponse(p_request, value);

        p_memory.pinning().unpinCID(p_request.getCid());

        return response;
    }

    static SignalResponse handleRemoveWithKeyRequest(@NotNull final RemoveRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_WITH_KEY_REQ;

        boolean result = Bucket.remove(p_memory.rawRead(), p_memory.rawWrite(), pin(p_memory, p_request.getCid()), p_request.getKey(), p_request.getValue());

        SignalResponse response = new SignalResponse(p_request,
                (result ? DataStructureMessageTypes.SUBSUBTYPE_SUCCESS : DataStructureMessageTypes.SUBSUBTYPE_ERROR));

        p_memory.pinning().unpinCID(p_request.getCid());

        return response;
    }


    public synchronized long size() {
        return Metadata.getHashMapSize(m_reader, m_metaData_address);
    }

    public synchronized boolean isEmpty() {
        return Metadata.getHashMapSize(m_reader, m_metaData_address) == 0;
    }


    public void clear() {
        clear(true);
    }

    private void clear(final boolean p_withInitializer) {
        m_lock.writeLock().lock(); // lock reentrant write lock

        clearAllBuckets();// remove all Buckets

        if (p_withInitializer) {

            long bucketCID = initBucket((short) 0, Bucket.getInitialMemorySize() + Metadata.getIndividualBucketSize(m_reader, pin(m_metaData_cid))); // allocate new Bucket
            m_memory.pinning().unpinCID(m_metaData_cid);
            clearHashtable(bucketCID); // set Hashtable to new Bucket CID

        }

        Metadata.clearSize(m_writer, m_metaData_address); // reset size

        m_lock.writeLock().unlock(); // unlock reentrant write lock
    }

    private void clearAllBuckets() {
        HashSet<Long> cids = Hashtable.bucketCIDs(m_memory.size(), m_reader, m_hashtable_cid, m_hashtable_address);

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

        for (short nodeId : grouped_cids.keySet()) {

            if (m_service.isLocal(nodeId)) { // local remove

                grouped_cids.get(nodeId).forEach((cid) -> m_memory.remove().remove(cid, false));

            } else { // global remove

                m_service.sendAsync(new ClearMessage(nodeId, Longs.toArray(grouped_cids.get(nodeId))));

            }
        }
    }

    private void clearHashtable(final long p_defaultEntry) {
        Hashtable.clear(m_memory.size(), m_writer, m_hashtable_cid, m_hashtable_address, p_defaultEntry);
    }


    private static void clearAllBucketsFromLocalMemory(final DXMem p_memory, @NotNull final long[] p_arr) {
        for (long cid : p_arr) {
            p_memory.remove().remove(cid);
        }
    }


    static void handleClearRequest(@NotNull final ClearMessage p_message, final DXMem p_memory) {
        clearAllBucketsFromLocalMemory(p_memory, p_message.getCids());
    }

    public V getOrDefault(final K p_key, final V p_defaultValue) {
        V ret = this.get(p_key);
        return ret == null ? p_defaultValue : ret;
    }

    static boolean assertInitialParameter(final String p_name, final int p_initialCapacity, final List<Short> p_onlinePeers,
                                          final int p_numberOfNodes, final short p_keyBytes, final short p_valueBytes,
                                          final byte p_hashFunctionId) {
        return p_initialCapacity > 1 && (p_numberOfNodes > 0 || p_numberOfNodes == -1) && p_keyBytes > 0 &&
                p_valueBytes > 0 && HashFunctions.isProperValue(p_hashFunctionId) && p_onlinePeers.size() > 0 &&
                NameserviceComponent.hasCorrectNameFormat(p_name);
    }


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

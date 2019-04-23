package de.hhu.bsinfo.dxram.datastructure;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.primitives.Longs;
import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.Pinning;
import de.hhu.bsinfo.dxmem.operations.RawRead;
import de.hhu.bsinfo.dxmem.operations.RawWrite;
import de.hhu.bsinfo.dxram.datastructure.messages.*;
import de.hhu.bsinfo.dxram.datastructure.util.*;
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

    static {
        HASH_TABLE_DEPTH_LIMIT = 31;
        BUCKET_ENTRIES_EXP = 2;
        BUCKET_ENTRIES = (int) Math.pow(2, BUCKET_ENTRIES_EXP);
        BUCKET_INITIAL_DEPTH = 0;
        SKEMA_DEFAULT_ID = -1;
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

        // calculate hashtable_depth
        short hashtable_depth = calcTableDepth(p_initialCapacity);

        // Init NodePool
        m_nodepool_cid = initNodePool(p_onlinePeers, p_numberOfNodes);

        // Init Bucket
        long bucket_cid = initBucket(BUCKET_INITIAL_DEPTH, Bucket.calcIndividualBucketSize(HashMap.BUCKET_ENTRIES, p_keyBytes, p_valueBytes));

        // Inti Hashtable
        m_hashtable_cid = initHashtable(hashtable_depth, bucket_cid);
        m_hashtable_address = m_memory.pinning().pin(m_hashtable_cid).getAddress();

        // Init Metadata
        m_metaData_cid = initMetaData(m_nodepool_cid, m_hashtable_cid, Bucket.calcIndividualBucketSize(BUCKET_ENTRIES, p_keyBytes, p_valueBytes), p_hashFunctionId);
        m_metaData_address = m_memory.pinning().pin(m_metaData_cid).getAddress();

        // Register Metadata
        m_service.registerHashMap(m_metaData_cid, p_name);
    }

    // TODO:
    boolean removeThisObject() {
        // TODO: Clear all buckets

        // TODO: Clear nodepool

        // TODO: Clear table and unpin them
        m_memory.pinning().unpinCID(m_hashtable_address);

        // TODO: Clear mapData and unpin
        m_memory.pinning().unpinCID(m_metaData_address);
        return true;
    }


    private long initNodePool(@NotNull final List<Short> p_onlinePeers, final int p_numberOfNodes) {
        long cid = m_memory.create().create(NodePool.getInitialMemorySize(p_onlinePeers.size(), p_numberOfNodes));
        long address = lockAndPin(cid);
        NodePool.initialize(m_writer, p_onlinePeers, address, p_numberOfNodes);
        log.info(NodePool.toString(m_memory.size(), m_reader, cid, address));
        unlockAndUnpin(cid);
        return cid;
    }

    private long initBucket(final short p_depth, final int p_individualBucketSize) {
        int size = Bucket.getInitialMemorySize() + p_individualBucketSize;
        long cid = m_memory.create().create(size);
        long address = lockAndPin(cid);
        Bucket.initialize(m_writer, address, p_depth);
        log.info(Bucket.toString(m_memory.size(), m_reader, cid, address));
        unlockAndUnpin(cid);
        return cid;
    }

    private long initHashtable(final short p_depth, final long p_defaultEntry) {
        int initialSize = Hashtable.getInitialMemorySize(p_depth);
        long cid = m_memory.create().create(initialSize);
        long address = lockAndPin(cid);
        Hashtable.initialize(m_writer, address, initialSize, p_depth, p_defaultEntry);
        log.info(Hashtable.toString(m_memory.size(), m_reader, cid, address));
        unlockAndUnpin(cid);
        return cid;
    }

    private long initMetaData(final long p_nodePool_cid, final long p_hashtable_cid, final int p_individual_bucketSize,
                              final byte p_hashFunctionId) {
        long cid = m_memory.create().create(Metadata.getInitialMemorySize());
        long address = lockAndPin(cid);
        Metadata.initialize(m_writer, address, p_hashtable_cid, p_nodePool_cid, 0L, SKEMA_DEFAULT_ID, p_individual_bucketSize, p_hashFunctionId);
        log.info(Metadata.toString(m_memory.size(), m_reader, cid, address));
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

        // Skema registration
        if (m_skemaRegistrationSwitch) {
            Metadata.setSkemaValueType(m_writer, m_metaData_address, Skema.resolveIdentifier(p_value.getClass()));
            m_skemaRegistrationSwitch = false;
        }

        // Serialize
        final byte[] key = Skema.serialize(p_key);
        final byte[] value = Skema.serialize(p_value);

        // Hash key
        final byte[] hash = HashFunctions.hash(Metadata.getHashFunctionId(m_reader, m_metaData_address), key);

        // reentrant write lock
        m_lock.writeLock().lock();

        // write lock on hashtable
        //m_memory.lock().lock(m_hashtable_cid, true, -1);

        // Lookup in Hashtable
        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);


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

                // put local and save result
                result = putLocal(address, p_cid, p_hash, p_key, p_value);

                //log.info(Bucket.toString(m_memory.size(), m_reader, p_cid, m_memory.pinning().translate(p_cid)));

                m_memory.pinning().unpinCID(p_cid);

            } else {

                PutRequest request = new PutRequest(ChunkID.getCreatorID(p_cid), p_key, p_value, Hashtable.getDepth(m_reader, m_hashtable_address), p_cid, m_hashFunctionId);
                PutResponse response = (PutResponse) m_service.sendSync(request, -1);
                result = consumePutResponse(response);
                if (result.m_error)
                    log.error("Global");

            }

            // handle optional resize
            if (result.m_resized) {
                m_hashtable_address = Hashtable.resize(m_reader, m_writer, m_memory.resize(), m_memory.pinning(), m_hashtable_cid, m_hashtable_address);
                p_index = ExtendibleHashing.extendibleHashing(p_hash, Hashtable.getDepth(m_reader, m_hashtable_address));
                p_cid = Hashtable.lookup(m_reader, m_hashtable_address, p_index);
            }

            // handle optional bucket split
            if (result.m_new_bucket != ChunkID.INVALID_ID) {
                Hashtable.splitForEntry(m_reader, m_writer, m_memory.size(), m_hashtable_cid, m_hashtable_address, p_cid, p_index, result.m_new_bucket);
                p_cid = Hashtable.lookup(m_reader, m_hashtable_address, p_index);
            }

            // handle error
            if (result.m_error) {
                m_lock.writeLock().unlock();
                log.error("Could not put key into HashMap\nKey = " + Arrays.toString(p_key));
                return false;
            }

            if (result.m_success && !result.m_overwrite) {
                // Metadata increment size
                Metadata.incrementSize(m_writer, m_reader, m_metaData_address);
            }

        } while (!result.m_success);

        // unlock reentrant write lock
        m_lock.writeLock().unlock();

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
        if (!Bucket.isFull(m_reader, p_address)) {

            // save put with optional resize
            HashMap.Result result = savePut(m_memory, p_address, p_bucketCID, p_key, p_value);

            if (!result.m_success)
                log.warn("savePut failed for key: " + Arrays.toString(p_key));

            return result;

        } else {

            // Bucket has maximum entries
            short bucket_depth = Bucket.getDepth(m_reader, p_address);

            int table_depth = Hashtable.getDepth(m_reader, m_hashtable_address);

            // compare depth to decide
            if (bucket_depth < table_depth) {

                // split bucket
                return splitBucketAndPut(p_address, p_bucketCID, p_hash, m_hashFunctionId, p_key, p_value);

            } else if (bucket_depth == table_depth) {

                // resize call
                HashMap.Result result = new HashMap.Result();
                result.m_resized = true;
                return result;

            } else
                throw new RuntimeException();

        }
    }

    private HashMap.Result splitBucketAndPut(final long p_address, final long p_bucketCID, final byte[] p_hash, final byte p_hashFunctionId, final byte[] p_key, final byte[] p_value) {
        // allocate bucket
        long cid = allocateCID(m_memory.size().size(p_bucketCID));

        HashMap.Result result = new HashMap.Result();

        // split bucket
        if (m_service.isLocal(cid)) {

            // allocated chunk is on this peer
            long address2 = pin(cid);
            result = splitBucketAndPutLocalMemory(m_memory, p_address, address2, p_bucketCID, cid, p_hash, p_hashFunctionId, p_key, p_value);
            m_memory.pinning().unpinCID(cid);

        } else {

            // allocated chunk is not on this peer
            Bucket.BucketRawData rawData = Bucket.splitBucket(m_reader, m_writer, p_address, p_hashFunctionId);

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

            // sendMessage with rawData
            if (rawData.getByteArray() == null) {
                log.error("rawData Bytes from Bucketsplit are null");
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

        // check if space from bucket is enough
        if (!Bucket.isEnoughSpace(p_memory.rawRead(), p_memory.size(), p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value))) {

            log.debug("RESIZE for ChunkID = " + ChunkID.toHexString(p_bucketCID));

            // resize Bucket and don't forget unpin, pin and update address
            int new_size = Bucket.sizeForFit(p_memory.rawRead(), p_memory.size(), p_bucketCID, p_address, Bucket.calcStoredSize(p_key, p_value));

            p_memory.resize().resize(p_bucketCID, new_size);

            p_address = p_memory.pinning().translate(p_bucketCID);
            // todo: handle if it's not working --> migrate bucket and try again and return false
        }

        if (Bucket.contains(p_memory.rawRead(), p_address, p_key)) {

            // overwrite value by remove this entry and recall ths function
            result.m_overwrite = true;
            Bucket.remove(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_key);
            result.m_success = savePut(p_memory, p_address, p_bucketCID, p_key, p_value).m_success; // should never be false
            if (!result.m_success)
                throw new RuntimeException();

        }

        // put into bucket
        Bucket.put(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_key, p_value);
        result.m_success = true;


        log.debug("Save Put -->\nBucket after put the key = " + Arrays.toString(p_key) + "\nand value = "
                + Arrays.toString(p_value) + "\n" +
                Bucket.toString(p_memory.size(), p_memory.rawRead(), p_bucketCID, p_address) + "\n");

        return result;
    }

    private static HashMap.Result splitBucketAndPutLocalMemory(@NotNull final DXMem p_memory, final long p_address, final long p_address2,
                                                               final long p_bucketCID, final long p_newBucketCID,
                                                               final byte[] p_hash, final byte p_hashFunctionId,
                                                               final byte[] p_key, final byte[] p_value) {
        log.debug("Bucket Split for Bucket = " + ChunkID.toHexString(p_bucketCID) +
                "\nnew Bucket = " + ChunkID.toHexString(p_newBucketCID) + "\n");

        // split bucket
        Bucket.splitBucket(p_memory.rawRead(), p_memory.rawWrite(), p_address, p_address2, p_hashFunctionId);

        // try put, true --> new bucket else old bucket
        if (ExtendibleHashing.compareBitForDepth(p_hash, Bucket.getDepth(p_memory.rawRead(), p_address))) {
            //log.debug(Bucket.toString(p_memory.size(), p_memory.rawRead(), p_newBucketCID, p_address2));
            if (!Bucket.isFull(p_memory.rawRead(), p_address2))
                return savePut(p_memory, p_address2, p_newBucketCID, p_key, p_value);

        } else {
            //log.debug(Bucket.toString(p_memory.size(), p_memory.rawRead(), p_bucketCID, p_address));
            if (!Bucket.isFull(p_memory.rawRead(), p_address))
                return savePut(p_memory, p_address, p_bucketCID, p_key, p_value);
        }

        log.debug("Bucket Split was ok but creates no space for put\n");

        return new HashMap.Result(); // As caller add the bucket cid to result
    }


    @NotNull
    @Contract("_, _ -> new")
    static PutResponse handlePutRequest(@NotNull final PutRequest p_request, final DXMem p_memory) {

        // check for resize call as fast as possible
        long address = pin(p_memory, p_request.getCid());
        boolean isFull = Bucket.isFull(p_memory.rawRead(), address);
        short bucket_depth = Bucket.getDepth(p_memory.rawRead(), address);

        if (isFull && bucket_depth == p_request.getTableDepth()) {
            p_memory.pinning().unpinCID(p_request.getCid());
            return new PutResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_RESIZE, ChunkID.INVALID_ID);
        }

        // global put
        HashMap.Result result = putGlobal(p_memory, address, isFull, bucket_depth, p_request.getTableDepth(),
                p_request.getCid(), p_request.getHashedKey(), p_request.getKey(), p_request.getValue(),
                p_request.getHashFunctionId());

        // unpin cid
        p_memory.pinning().unpinCID(p_request.getCid());

        return new PutResponse(p_request, result.getSubSubType(), result.m_new_bucket);
    }

    private static HashMap.Result putGlobal(final DXMem p_memory, final long p_address, final boolean p_isFull,
                                            final short p_depth, final short p_tableDepth, final long p_cid,
                                            final byte[] p_hash, final byte[] p_key, final byte[] p_value,
                                            final byte p_hashFunctionId) {

        HashMap.Result result;
        if (!p_isFull) {

            // save put with optional resize
            result = savePut(p_memory, p_address, p_cid, p_key, p_value);

        } else if (p_depth < p_tableDepth) {

            // allocate
            long cid2 = p_memory.create().create(p_memory.size().size(p_cid));
            long address2 = pin(p_memory, cid2);

            // bucket split
            result = splitBucketAndPutLocalMemory(p_memory, p_address, address2, p_cid, cid2, p_hash, p_hashFunctionId, p_key, p_value);
            result.m_new_bucket = cid2;

            // unpin and unlock new cid
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

        // Never a bad signal, maybe add this in future
        return new SignalResponse(p_request, DataStructureMessageTypes.SUBSUBTYPE_SUCCESS);

    }


    public V get(final K p_key) {
        assert p_key != null;

        // Serialize
        final byte[] key = Skema.serialize(p_key);

        // Hash key
        final byte[] hash = HashFunctions.hash(Metadata.getHashFunctionId(m_reader, m_metaData_address), key);
        //log.info("Key: " + Arrays.toString(key));
        //log.info("Hashed Key: " + Arrays.toString(hash));

        // lock reentrant write lock
        m_lock.readLock().lock();

        //log.info(Hashtable.toString(m_memory.size(), m_reader, m_hashtable_cid, m_hashtable_address));

        // Lookup in Hashtable
        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);

        //log.info("Index = " + index);
        //log.info("CID = " + ChunkID.toHexString(cid));

        final V value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaData_address));

        byte[] valueBytes;

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

        // lock reentrant write lock
        m_lock.readLock().unlock();

        log.warn(Arrays.toString(valueBytes));

        if (valueBytes == null)
            throw new NullPointerException();

        Skema.deserialize(value, valueBytes);

        return value;
    }


    private static byte[] getFromLocalMemory(@NotNull final DXMem p_memory, final long p_cid, final byte[] p_key) {
        long address = p_memory.pinning().pin(p_cid).getAddress();

        //log.info(Bucket.toString(p_memory.size(), p_memory.rawRead(), p_cid, address));

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

        // Serialize
        final byte[] key = Skema.serialize(p_key);

        // Hash key
        final byte[] hash = HashFunctions.hash(Metadata.getHashFunctionId(m_reader, m_metaData_address), key);

        //log.info("key = " + Arrays.toString(key));

        // lock reentrant write lock
        m_lock.writeLock().lock();

        // Lookup in Hashtable
        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);

        final V value = Skema.newInstance(Metadata.getSkemaValueId(m_reader, m_metaData_address));

        log.debug("Remove is : " + (m_service.isLocal(cid) ? "local" : "global") + "\n");

        byte[] valueBytes;
        if (m_service.isLocal(cid)) {

            long address = m_memory.pinning().pin(cid).getAddress();

            valueBytes = Bucket.remove(m_reader, m_writer, address, key);

            //log.info(Bucket.toString(m_memory.size(), m_reader, cid, address));

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

        // Metadata decrement size
        Metadata.decrementSize(m_writer, m_reader, m_metaData_address);

        // unlock reentrant write lock
        m_lock.writeLock().unlock();

        Skema.deserialize(value, valueBytes);

        return value;
    }

    public boolean remove(final K p_key, final V p_value) {
        assert p_key != null && p_value != null;

        // Serialize
        final byte[] key = Skema.serialize(p_key);
        final byte[] value = Skema.serialize(p_value);

        // Hash key
        final byte[] hash = HashFunctions.hash(Metadata.getHashFunctionId(m_reader, m_metaData_address), key);

        // lock reentrant write lock
        m_lock.writeLock().lock();

        // Lookup in Hashtable
        final int index = ExtendibleHashing.extendibleHashing(hash, Hashtable.getDepth(m_reader, m_hashtable_address));
        final long cid = Hashtable.lookup(m_reader, m_hashtable_address, index);

        log.debug("Remove is : " + (m_service.isLocal(cid) ? "local" : "global") + "\n");

        boolean result;
        if (m_service.isLocal(cid)) {

            long address = m_memory.pinning().pin(cid).getAddress();

            result = Bucket.remove(m_reader, m_writer, address, key, value);

            //log.info(Bucket.toString(m_memory.size(), m_reader, cid, address));

            m_memory.pinning().unpinCID(cid);

        } else {

            RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(cid), key, value, cid);

            SignalResponse response = (SignalResponse) m_service.sendSync(request, -1);

            result = response.wasSuccessful();
        }

        if (result) {
            // Metadata decrement size
            Metadata.decrementSize(m_writer, m_reader, m_metaData_address);
        }

        // unlock reentrant write lock
        m_lock.writeLock().unlock();

        return result;
    }


    static RemoveResponse handleRemoveRequest(@NotNull final RemoveRequest p_request, @NotNull final DXMem p_memory) {
        assert p_request.getSubtype() == DataStructureMessageTypes.SUBTYPE_REMOVE_REQ;

        long address = pin(p_memory, p_request.getCid());
        log.debug(Bucket.toString(p_memory.size(), p_memory.rawRead(), p_request.getCid(), address));

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
        // lock reentrant write lock
        m_lock.writeLock().lock();

        // remove all Buckets
        clearAllBuckets();

        // allocate new Bucket
        long bucketCID = initBucket((short) 0, Bucket.getInitialMemorySize() + Metadata.getIndividualBucketSize(m_reader, pin(m_metaData_cid)));
        m_memory.pinning().unpinCID(m_metaData_cid);

        // set Hashtable to new Bucket CID
        clearHashtable(bucketCID);

        // reset size
        Metadata.clearSize(m_writer, m_metaData_address);

        // visualize
        //log.error(Hashtable.toString(m_memory.size(), m_reader, m_hashtable_cid, m_hashtable_address));

        // unlock reentrant write lock
        m_lock.writeLock().unlock();
    }

    private void clearAllBuckets() {
        HashSet<Long> cids = Hashtable.bucketCIDs(m_memory.size(), m_reader, m_hashtable_cid, m_hashtable_address);

        java.util.HashMap<Short, ArrayList<Long>> grouped_cids = new java.util.HashMap<>(cids.size());

        // group_cids
        cids.forEach((cid) -> {
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

            if (m_service.isLocal(nodeId)) {

                // local remove
                grouped_cids.get(nodeId).forEach((cid) -> m_memory.remove().remove(cid, false));

            } else {

                // remote remove
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


    // TODO:
    public Set<K> keySet() {
        return null;
    }

    // TODO:
    public Set<Map.Entry<K, V>> entrySet() {
        return null;
    }

    public V getOrDefault(final K p_key, final V p_defaultValue) {
        V ret = this.get(p_key);
        return ret == null ? p_defaultValue : ret;
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

package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;

/**
 * This class could only be used while the Memory is pinned. It represents a memory layout for the meta data of the HashMap
 * <p>
 * Memory
 * +------------+
 * | (2 bytes)* |
 * +------------+
 * <p>
 *
 * @see de.hhu.bsinfo.dxram.datastructure.HashMap
 * @see de.hhu.bsinfo.dxmem.operations.RawWrite
 * @see de.hhu.bsinfo.dxmem.operations.RawRead
 * @see de.hhu.bsinfo.dxmem.operations.Size
 **/
@PinnedMemory
@NoParamCheck
class Metadata {

    private static int HASHTABLE_CID_OFFSET;
    private static int NODEPOOL_CID_OFFSET;
    private static byte HASHFUNCTION_ID_OFFSET;
    private static int HASHMAP_SIZE_OFFSET;
    private static int SKEMA_VALUE_TYPE_OFFSET;
    private static int INDIVIDUAL_BUCKET_SIZE_OFFSET;

    private static int MEM_SIZE;

    static {
        HASHTABLE_CID_OFFSET = 0;
        NODEPOOL_CID_OFFSET = 8;
        HASHFUNCTION_ID_OFFSET = 16;
        HASHMAP_SIZE_OFFSET = 17;
        SKEMA_VALUE_TYPE_OFFSET = 25;
        INDIVIDUAL_BUCKET_SIZE_OFFSET = 27;

        MEM_SIZE = 31;
    }

    /**
     * Returns the memory size for this memory layout.
     *
     * @return the memory size for this memory layout.
     */
    static int getInitialMemorySize() {
        return MEM_SIZE;
    }

    /**
     * Initializes the Metadata by writing all given values into the memory.
     *
     * @param p_writer               DXMem writer for direct memory access.
     * @param p_address              where the metadata should be stored.
     * @param p_table                ChunkID of the Hashtable.
     * @param p_nodepool             ChunkID of the Nodepool.
     * @param p_size                 number of stored key-value pairs in the HashMap
     * @param p_skemaId              ID for the datatype of the value, to deserialize it.
     * @param p_individualBucketSize which will be used for an allocation of a new bucket.
     * @param p_hashFunctionId       ID for the hash algorithm which the HashMap use.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void initialize(final RawWrite p_writer, final long p_address, final long p_table, final long p_nodepool,
                           final long p_size, final short p_skemaId, final int p_individualBucketSize,
                           final byte p_hashFunctionId) {
        p_writer.writeLong(p_address, HASHTABLE_CID_OFFSET, p_table);
        p_writer.writeLong(p_address, NODEPOOL_CID_OFFSET, p_nodepool);
        p_writer.writeByte(p_address, HASHFUNCTION_ID_OFFSET, p_hashFunctionId);
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_size);
        p_writer.writeShort(p_address, SKEMA_VALUE_TYPE_OFFSET, p_skemaId);
        p_writer.writeInt(p_address, INDIVIDUAL_BUCKET_SIZE_OFFSET, p_individualBucketSize);
    }

    /**
     * Sets the id for the value type for the serializer skema.
     *
     * @param p_writer  DXMem writer for direct memory access.
     * @param p_address where the metadata are stored.
     * @param p_skemaId ID for the datatype of the value, to deserialize it.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void setSkemaValueType(final RawWrite p_writer, final long p_address, final short p_skemaId) {
        p_writer.writeShort(p_address, SKEMA_VALUE_TYPE_OFFSET, p_skemaId);
    }

    /**
     * Returns the ChunkID where the Hashtable is stored.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the ChunkID where the Hashtable is stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static long getHashtable(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, HASHTABLE_CID_OFFSET);
    }

    /**
     * Returns the ChunkID where the NodePool is stored.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the ChunkID where the NodePool is stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static long getNodepool(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, NODEPOOL_CID_OFFSET);
    }

    /**
     * Returns the ID for the used hash algorithm.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the ID for the used hash algorithm.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static byte getHashFunctionId(final RawRead p_reader, final long p_address) {
        return p_reader.readByte(p_address, HASHFUNCTION_ID_OFFSET);
    }

    /**
     * Returns the number of stored key-value pairs in the HashMap.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the number of stored key-value pairs in the HashMap.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static long getHashMapSize(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET);
    }

    /**
     * Returns the ID for the dataype of the value.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the ID for the dataype of the value.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static short getSkemaValueId(final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SKEMA_VALUE_TYPE_OFFSET);
    }

    /**
     * Returns the individual size of a bucket.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @return the individual size of a bucket.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static int getIndividualBucketSize(final RawRead p_reader, final long p_address) {
        return p_reader.readInt(p_address, INDIVIDUAL_BUCKET_SIZE_OFFSET);
    }

    /**
     * Increments the number of stored key-value pairs in the HashMap.
     *
     * @param p_writer  DXMem writer for direct memory access.
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static void incrementSize(final RawWrite p_writer, final RawRead p_reader, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET) + 1L);
    }

    /**
     * Decrements the number of stored key-value pairs in the HashMap.
     *
     * @param p_writer  DXMem writer for direct memory access.
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the metadata are stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static void decrementSize(final RawWrite p_writer, final RawRead p_reader, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET) - 1L);
    }

    /**
     * Resets the number of stored key-value pairs in the HashMap to 0.
     *
     * @param p_writer  DXMem writer for direct memory access.
     * @param p_address where the metadata are stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void clearSize(final RawWrite p_writer, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, 0L);

    }

    /**
     * Returns a representation of this metadata as String.
     *
     * @param p_size    DXMem size-operation.
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_cid     ChunkID of the metadata.
     * @param p_address where the metadata are stored.
     * @return
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static String toString(final Size p_size, final RawRead p_reader, final long p_cid, final long p_address) {
        StringBuilder builder = new StringBuilder();

        // add allocated Size
        int chunk_size = p_size.size(p_cid);
        builder.append("\n*********************************************************************************************\n");
        builder.append("Metadata Chunk Size: " + chunk_size + " Bytes\n");

        // add Data
        builder.append("***DATA***\n");
        builder.append("Nodepool CID: " + ChunkID.toHexString(p_reader.readLong(p_address, NODEPOOL_CID_OFFSET)) + "\n");
        builder.append("Hashtable CID: " + ChunkID.toHexString(p_reader.readLong(p_address, HASHTABLE_CID_OFFSET)) + "\n");
        builder.append("Hashfunction ID: " + p_reader.readByte(p_address, HASHFUNCTION_ID_OFFSET) + "\n");
        builder.append("HashMap Size: " + p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET) + "\n");
        builder.append("SkemaValue ID: " + p_reader.readShort(p_address, SKEMA_VALUE_TYPE_OFFSET) + "\n");
        builder.append("IndividualBucketSize: " + p_reader.readInt(p_address, INDIVIDUAL_BUCKET_SIZE_OFFSET) + "\n");

        builder.append("*********************************************************************************************\n");

        return builder.toString().trim();
    }

}

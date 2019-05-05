package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;

/**
 * This class could only be used while the Memory is pinned.
 * <p>
 * Memory
 * +------------+
 * | (2 bytes)* |
 * +------------+
 * <p>
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

    static int getInitialMemorySize() {
        return MEM_SIZE;
    }

    static void initialize(final RawWrite p_writer, final long p_address, final long p_table, final long p_nodepool,
                           final long p_size, final short p_skemaId, final int p_individual_bucket_size,
                           final byte p_hashFunctionId) {
        p_writer.writeLong(p_address, HASHTABLE_CID_OFFSET, p_table);
        p_writer.writeLong(p_address, NODEPOOL_CID_OFFSET, p_nodepool);
        p_writer.writeByte(p_address, HASHFUNCTION_ID_OFFSET, p_hashFunctionId);
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_size);
        p_writer.writeShort(p_address, SKEMA_VALUE_TYPE_OFFSET, p_skemaId);
        p_writer.writeInt(p_address, INDIVIDUAL_BUCKET_SIZE_OFFSET, p_individual_bucket_size);
    }

    static void setSkemaValueType(final RawWrite p_writer, final long p_address, final short p_skemaId) {
        p_writer.writeShort(p_address, SKEMA_VALUE_TYPE_OFFSET, p_skemaId);
    }

    static long getHashtable(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, HASHTABLE_CID_OFFSET);
    }

    static long getNodepool(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, NODEPOOL_CID_OFFSET);
    }

    static byte getHashFunctionId(final RawRead p_reader, final long p_address) {
        return p_reader.readByte(p_address, HASHFUNCTION_ID_OFFSET);
    }

    static long getHashMapSize(final RawRead p_reader, final long p_address) {
        return p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET);
    }

    static short getSkemaValueId(final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SKEMA_VALUE_TYPE_OFFSET);
    }

    static int getIndividualBucketSize(final RawRead p_reader, final long p_address) {
        return p_reader.readInt(p_address, INDIVIDUAL_BUCKET_SIZE_OFFSET);
    }

    static void incrementSize(final RawWrite p_writer, final RawRead p_reader, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET) + 1L);
    }

    static void decrementSize(final RawWrite p_writer, final RawRead p_reader, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, p_reader.readLong(p_address, HASHMAP_SIZE_OFFSET) - 1L);
    }

    static void clearSize(final RawWrite p_writer, final long p_address) {
        p_writer.writeLong(p_address, HASHMAP_SIZE_OFFSET, 0L);

    }

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

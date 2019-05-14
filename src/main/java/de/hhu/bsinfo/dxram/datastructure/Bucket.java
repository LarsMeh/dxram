package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;
import de.hhu.bsinfo.dxram.datastructure.util.ConverterLittleEndian;
import de.hhu.bsinfo.dxram.datastructure.util.ExtendibleHashing;
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class could only be used while the Memory is pinned. It represents a memory layout of a bucket. A bucktet is
 * a construct from the hash algorithm ExtendibleHashing. It holds a number of key-value pairs.
 * <p>
 * Memory
 * +------------------------------------------------------------------------+
 * | 4 Bytes used bytes :: 2 Bytes Depth :: 2 Bytes Size :: (Bytes Arrays)* |
 * +------------------------------------------------------------------------+
 * <p>
 * ByteArray
 * +-------------------------+
 * | 2 Bytes length :: Bytes |
 * +-------------------------+
 * <p>
 * Key or Value could only be 2^15 bytes big
 *
 * @see de.hhu.bsinfo.dxram.datastructure.HashMap
 * @see de.hhu.bsinfo.dxmem.operations.RawWrite
 * @see de.hhu.bsinfo.dxmem.operations.RawRead
 * @see de.hhu.bsinfo.dxmem.operations.Size
 * @see de.hhu.bsinfo.dxmem.operations.Resize
 **/
@PinnedMemory
@NoParamCheck
class Bucket {

    private static final Logger log = LogManager.getFormatterLogger(Bucket.class);

    private static final int USED_BYTES_OFFSET;
    private static final int DEPTH_OFFSET;
    private static final int SIZE_OFFSET;
    private static final int DATA_OFFSET;

    private static final int LENGTH_BYTES;

    static {
        USED_BYTES_OFFSET = 0;
        DEPTH_OFFSET = 4; // 32 bit
        SIZE_OFFSET = 6; // 48 bit
        DATA_OFFSET = 8; // 64 bit

        LENGTH_BYTES = 2; // Short
    }

    /**
     * This static class represents a raw format for a bucket. With method {@link #getByteArray()} you get the raw
     * format as bytes for sending them over the network. The class is similar to a Builder.
     */
    static class RawData {

        private static int HEADER_BYTES = Short.BYTES * 2;

        private ByteArrayOutputStream m_byteStream;
        private short m_size;
        private short m_depth;
        private int m_dataBytes;
        private byte[] m_rawBucket;

        /**
         * Returns the raw data as byte array.
         *
         * @return the raw data as byte array.
         */
        byte[] getByteArray() {
            return m_rawBucket;
        }

        void finish() {
            appendHeader();
            m_rawBucket = m_byteStream.toByteArray();
        }

        /**
         * Appends a key with his length. A key indicates a new key-value pair, so the size will be increased.
         *
         * @param p_length length of param p_bytes
         * @param p_bytes  key as byte array
         */
        void appendKey(final short p_length, final byte[] p_bytes) {
            m_size++;
            append(p_length, p_bytes);
        }

        /**
         * Appends a value with his length. This method should be called after {@link #appendKey(short, byte[])}.
         * The reason why the length is a paramter is that the length will be read as short from the memory. Then to discard
         * the short and casts the length from p_bytes to short needs longer. If the length field will be changed to Integer
         * there is no reason for a parameter length.
         *
         * @param p_length length of param p_bytes
         * @param p_bytes  value as byte array
         */
        void appendValue(final short p_length, final byte[] p_bytes) {
            append(p_length, p_bytes);
        }

        /**
         * Returns true if size is equal to 0. That means {@link #appendKey(short, byte[])} was never called before.
         *
         * @return true if field size == 0
         */
        boolean isEmpty() {
            return m_size == 0;
        }

        /**
         * Returns the data without the header.
         *
         * @return the data without the header.
         */
        int getDataBytes() {
            return m_dataBytes;
        }

        /**
         * Constructs a RawData object by initialize a {@link java.io.ByteArrayOutputStream}.
         *
         * @see java.io.ByteArrayOutputStream
         */
        private RawData() {
            m_byteStream = new ByteArrayOutputStream();
        }

        /**
         * Private method which appends a byte array with his length by writing it into the stream.
         * Methods {@link #appendKey(short, byte[])}, {@link #appendValue(short, byte[])} will call this.
         *
         * @param p_length length of parameter p_bytes
         * @param p_bytes  byte array which will written into the stream
         */
        private void append(final short p_length, final byte[] p_bytes) {
            try {

                m_byteStream.write(ConverterLittleEndian.shortToByteArray(p_length));
                m_byteStream.write(p_bytes);

            } catch (IOException p_e) {

                p_e.printStackTrace();
            }
            m_dataBytes += Short.BYTES + p_length;
        }

        /**
         * Appends the header size and depth for this bucket by writing them into the stream. This method will close the stream.
         */
        private void appendHeader() {
            try {

                m_byteStream.write(ConverterLittleEndian.shortToByteArray(m_size));
                m_byteStream.write(ConverterLittleEndian.shortToByteArray(m_depth));
                m_byteStream.close();

            } catch (IOException p_e) {

                p_e.printStackTrace();

            }
        }


        /**
         * Initializes the bucket. It will extract the information from the raw format and will write into the memory.
         *
         * @param p_writer       DXMem writer for direct memory access
         * @param p_address      where the bucket is stored
         * @param p_rawDataBytes bucket in raw format
         * @see de.hhu.bsinfo.dxmem.operations.RawWrite
         */
        private static void initialize(@NotNull final RawWrite p_writer, final long p_address, @NotNull final byte[] p_rawDataBytes) {
            p_writer.writeInt(p_address, USED_BYTES_OFFSET, DATA_OFFSET + p_rawDataBytes.length - RawData.HEADER_BYTES);
            p_writer.writeShort(p_address, DEPTH_OFFSET, extractDepth(p_rawDataBytes));
            p_writer.writeShort(p_address, SIZE_OFFSET, extractSize(p_rawDataBytes));
            p_writer.write(p_address, DATA_OFFSET, p_rawDataBytes, 0, p_rawDataBytes.length - RawData.HEADER_BYTES);
        }

        /**
         * Extracts the depth from a raw format of a bucket.
         *
         * @param p_rawDataBytes bucket in raw format
         * @return extracted depth.
         */
        @Contract(pure = true)
        private static short extractDepth(final byte[] p_rawDataBytes) {
            return ConverterLittleEndian.byteArrayToShort(p_rawDataBytes, p_rawDataBytes.length - 1);
        }

        /**
         * Extracts the size from a raw format of a bucket.
         *
         * @param p_rawDataBytes bucket in raw format.
         * @return extracted size.
         */
        @Contract(pure = true)
        private static short extractSize(final byte[] p_rawDataBytes) {
            return ConverterLittleEndian.byteArrayToShort(p_rawDataBytes, p_rawDataBytes.length - 3);
        }
    }

    /**
     * Returns the initial memory size which this bucket needs.
     *
     * @param p_entries    maximum entries which the bcuket should store
     * @param p_keyBytes   size of a key
     * @param p_valueBytes size of a value
     * @return the initial memory size which this bucket needs.
     */
    @Contract(pure = true)
    static int getInitialMemorySize(final int p_entries, final short p_keyBytes, final short p_valueBytes) {
        return DATA_OFFSET + calcIndividualBucketSize(p_entries, p_keyBytes, p_valueBytes);
    }

    /**
     * Initializes the bucket by setting the header.
     *
     * @param p_writer  DXMem writer for direct memory access
     * @param p_address where the bucket is stored
     * @param p_depth   depth of the bucket
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void initialize(@NotNull final RawWrite p_writer, final long p_address, final short p_depth) {
        p_writer.writeInt(p_address, USED_BYTES_OFFSET, DATA_OFFSET);
        p_writer.writeShort(p_address, DEPTH_OFFSET, p_depth);
        p_writer.writeShort(p_address, SIZE_OFFSET, (short) 0);
    }

    /**
     * Initializes the bucket from a bucket in raw data format. This method will call by {@link #splitBucket(DXMem, long, long, byte, boolean)}.
     *
     * @param p_writer  DXMem writer for direct memory access
     * @param p_address where the bucket is stored
     * @param p_rawData bucket which will written to the memory
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    private static void initialize(@NotNull final RawWrite p_writer, final long p_address, @NotNull final RawData p_rawData) {
        // write Header
        p_writer.writeInt(p_address, USED_BYTES_OFFSET, DATA_OFFSET + p_rawData.m_dataBytes);
        p_writer.writeShort(p_address, DEPTH_OFFSET, p_rawData.m_depth);
        p_writer.writeShort(p_address, SIZE_OFFSET, p_rawData.m_size);

        if (p_rawData.m_size > 0) // write Data
            p_writer.write(p_address, DATA_OFFSET, p_rawData.m_byteStream.toByteArray());
    }


    /**
     * Initializer if you send the bucket over the network.
     *
     * @param p_writer       DXMem writer for direct memory access
     * @param p_address      where the bucket is stored
     * @param p_rawDataBytes bucket which will written to the memory
     */
    static void initialize(final RawWrite p_writer, final long p_address, final byte[] p_rawDataBytes) {
        RawData.initialize(p_writer, p_address, p_rawDataBytes);
    }

    /**
     * Returns true if the number of stored key-value pairs is equal to the maximum entries a bucket could have.
     *
     * @param p_reader  DXMem reader for direct memory access
     * @param p_address where the bucket is stored
     * @return true if the number of stored key-value pairs is equal to the maximum entries a bucket could have.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxram.datastructure.HashMap#BUCKET_ENTRIES
     */
    static boolean isFull(@NotNull final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SIZE_OFFSET) == HashMap.BUCKET_ENTRIES;
    }

    /**
     * Returns true if the bucket has stored a key which is equal to parameter p_key.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_key     which will look after
     * @return true if the bucket has stored a key which is equal to parameter p_key.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static boolean contains(@NotNull final DXMem p_memory, final long p_address, final byte[] p_key) {
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        if (size == 0)
            return false;

        int right_offset = DATA_OFFSET;
        int current_entry = 1;

        while (current_entry <= size) { // run through data

            // read length field for the key
            short length = p_memory.rawRead().readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;

            if (length == p_key.length)
                if (p_memory.rawCompare().compare(p_address, right_offset, p_key)) // compare keys
                    return true;

            right_offset += length;
            right_offset += LENGTH_BYTES + p_memory.rawRead().readShort(p_address, right_offset); // skip value
            current_entry++;
        }

        return false;
    }

    /**
     * Returns the memory size for the metadata of a bucket.
     *
     * @return the memory size for the metadata of a bucket.
     */
    @Contract(pure = true)
    static int getMetadataMemSize() {
        return DATA_OFFSET;
    }

    /**
     * Returns the stored depth of the bucket.
     *
     * @param p_reader  DXMem reader for direct memory access
     * @param p_address where the bucket is stored
     * @return the stored depth of the bucket.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static short getDepth(@NotNull final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, DEPTH_OFFSET);
    }

    /**
     * Returns the stored size of the bucket.
     *
     * @param p_reader  DXMem reader for direct memory access
     * @param p_address where the bucket is stored
     * @return the stored size of the bucket.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static short getSize(@NotNull final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SIZE_OFFSET);
    }

    /**
     * Returns the used bytes which occupy the stored key-value pairs in space and adds the header bytes. With
     * parameter p_withoutHeader the header bytes will not be added. The return value should at any time be less than
     * the return value of {@link de.hhu.bsinfo.dxmem.operations.Size#size(long)}. If not then was written over the
     * allocated memory size for this bucket.
     *
     * @param p_reader        DXMem reader for direct memory access
     * @param p_address       where the bucket is stored
     * @param p_withoutHeader indicates if the header bytes should be subtract or not
     * @return the used bytes which occupy the stored key-value pairs in space and adds the header bytes.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static int getUsedBytes(@NotNull final RawRead p_reader, final long p_address, final boolean p_withoutHeader) {
        return p_reader.readInt(p_address, USED_BYTES_OFFSET) - (p_withoutHeader ? DATA_OFFSET : 0);
    }

    /**
     * Determine if a put operation for a key-value pair will take more memory space than allocated.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_cid     ChunkID of the bucket
     * @param p_address where the bucket is stored
     * @param p_bytes   space which will need the key value pair
     * @return true if the key-value pair could be inserted without resize the bucket.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static boolean isEnoughSpace(@NotNull final DXMem p_memory, final long p_cid, final long p_address, final int p_bytes) {
        int max_size = p_memory.size().size(p_cid); // allocated size of the bucket
        int used = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET); // used bytes by the bucket

        return (max_size - used) >= p_bytes;
    }

    /**
     * Returns the size which needs the bucket to stored the given bytes.
     *
     * @param p_reader  DXMem reader for direct memory access
     * @param p_size    DXmem size-operation
     * @param p_cid     ChunkID of the bucket
     * @param p_address where the bucket is stored
     * @param p_bytes   space which will need the key value pair
     * @return the size which needs the bucket to stored the given bytes.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static int sizeForFit(@NotNull final RawRead p_reader, @NotNull final Size p_size, final long p_cid, final long p_address, final int p_bytes) {
        int max_size = p_size.size(p_cid); // allocated size of the bucket
        int space = max_size - p_reader.readInt(p_address, USED_BYTES_OFFSET); // space between max and used

        return space >= p_bytes ? max_size : max_size + p_bytes - space;
    }

    /**
     * Removes a key-value pair from this bucket. The key will identify the pair. The method returns the matching
     * value.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_key     key as byte array
     * @return the matching value or null if the pair is not stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    @Nullable
    static byte[] remove(@NotNull final DXMem p_memory, final long p_address, final byte[] p_key) {
        // read information
        int usedBytes = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET);
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        assert size > 0;

        int offset = DATA_OFFSET;
        int lastOffset;
        int current_entry = 1;

        while (current_entry <= size) { // run through data

            lastOffset = offset; // bring offset's together

            // read key
            short length = p_memory.rawRead().readShort(p_address, offset);
            offset += LENGTH_BYTES;

            if (length == p_key.length) { // compare length field

                if (p_memory.rawCompare().compare(p_address, offset, p_key)) { // compare key

                    offset += length;

                    // save value to return later
                    length = p_memory.rawRead().readShort(p_address, offset);
                    offset += LENGTH_BYTES;
                    byte[] stored_value = p_memory.rawRead().read(p_address, offset, length);
                    offset += length;

                    if (size - 1 > 0) // close space
                        copy(p_memory, p_address, offset, usedBytes - offset, lastOffset);

                    // update used bytes
                    p_memory.rawWrite().writeInt(p_address, USED_BYTES_OFFSET, usedBytes - (offset - lastOffset));

                    // update size
                    p_memory.rawWrite().writeShort(p_address, SIZE_OFFSET, (short) (size - 1));

                    return stored_value;
                }
            }

            // skip value
            offset += length;
            offset += LENGTH_BYTES + p_memory.rawRead().readShort(p_address, offset);

            // increment current entry
            current_entry++;
        }

        return null;
    }

    /**
     * Removes a key-value pair from this bucket. The key and the value will identify the pair. The method returns true
     * if a matching key-value pair is stored.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_key     key as byte array
     * @param p_value   value as byte array
     * @return the matching value or null if the pair is not stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     **/
    static boolean remove(@NotNull final DXMem p_memory, final long p_address, final byte[] p_key, final byte[] p_value) {
        // read information
        int usedBytes = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET);
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        assert size > 0;

        int right_offset = DATA_OFFSET;
        int left_offset;
        int current_entry = 1;

        while (current_entry <= size) { // run through data

            left_offset = right_offset; // bring offset's together

            // read key
            short length = p_memory.rawRead().readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;

            if (length == p_key.length) { // compare length field

                if (p_memory.rawCompare().compare(p_address, right_offset, p_key)) { // compare key

                    right_offset += length;

                    length = p_memory.rawRead().readShort(p_address, right_offset);
                    right_offset += LENGTH_BYTES;

                    if (p_memory.rawCompare().compare(p_address, right_offset, p_value)) { // compare values

                        right_offset += length;

                        if (size - 1 > 0) // close space
                            copy(p_memory, p_address, right_offset, usedBytes - right_offset, left_offset);

                        // update used bytes
                        p_memory.rawWrite().writeInt(p_address, USED_BYTES_OFFSET, usedBytes - (right_offset - left_offset));

                        // update size
                        p_memory.rawWrite().writeShort(p_address, SIZE_OFFSET, (short) (size - 1));

                        return true;

                    } else
                        right_offset += length;

                } else
                    right_offset += length;

            } else
                right_offset += LENGTH_BYTES + p_memory.rawRead().readShort(p_address, right_offset); // skip value

            current_entry++;
        }

        return false;
    }

    /**
     * Returns the matching value for parameter p_key or null if no key-value pair is stored.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_key     key as byte array
     * @return the matching value for parameter p_key or null if no key-value pair is stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    @Nullable
    static byte[] get(@NotNull final DXMem p_memory, final long p_address, final byte[] p_key) {
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        assert size > -1;

        int right_offset = DATA_OFFSET;
        int current_entry = 1;
        short length;

        while (current_entry <= size) { // run through data

            // read key
            length = p_memory.rawRead().readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;

            if (length == p_key.length) { // compare length field

                if (p_memory.rawCompare().compare(p_address, right_offset, p_key)) { // compare key

                    right_offset += length;
                    length = p_memory.rawRead().readShort(p_address, right_offset);
                    right_offset += LENGTH_BYTES;

                    return p_memory.rawRead().read(p_address, right_offset, length); // return value

                }
            }

            right_offset += length;
            right_offset += LENGTH_BYTES + p_memory.rawRead().readShort(p_address, right_offset); // skip value

            current_entry++;
        }

        return null;
    }

    /**
     * Puts a key-value pair in the bucket. The method will append the pair and will not check the memory bounds.
     * For correct usage call methods {@link #isEnoughSpace(DXMem, long, long, int)} and optional
     * {@link #sizeForFit(RawRead, Size, long, long, int)}.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_key     key as byte array
     * @param p_value   value as byte array
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void put(@NotNull final DXMem p_memory, final long p_address, @NotNull final byte[] p_key, @NotNull final byte[] p_value) {
        // read information
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        assert size >= 0;

        int offset = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET);

        // write key-value pair
        p_memory.rawWrite().writeShort(p_address, offset, (short) p_key.length);
        offset += LENGTH_BYTES;
        p_memory.rawWrite().write(p_address, offset, p_key);
        offset += p_key.length;
        p_memory.rawWrite().writeShort(p_address, offset, (short) p_value.length);
        offset += LENGTH_BYTES;
        p_memory.rawWrite().write(p_address, offset, p_value);

        // update used bytes
        int usedBytes = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET);
        usedBytes += calcStoredSize(p_key, p_value);
        p_memory.rawWrite().writeInt(p_address, USED_BYTES_OFFSET, usedBytes);

        p_memory.rawWrite().writeShort(p_address, SIZE_OFFSET, (short) (size + 1)); // update size
    }

    /**
     * Splits the bucket and the new bucket is local, so the data could be written to the memory.
     * Important: the allocated new bucket has to be same size than this bucket
     *
     * @param p_memory      DXMem instance to get direct access to the memory
     * @param p_own_address where the bucket is stored which will split
     * @param p_address     where the new bucket will stored to
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void splitBucket(final DXMem p_memory, final long p_own_address, final long p_address, final byte p_hashId) {
        splitBucket(p_memory, p_own_address, p_address, p_hashId, true);
    }

    /**
     * Splits the bucket and returns the raw data format from the new bucket. With parameter p_withInitializer will the function call {@link #initialize(RawWrite, long, de.hhu.bsinfo.dxram.datastructure.Bucket.RawData)}.
     * It indicates that the new bucket is local.
     * The incurred space will be closed.
     *
     * @param p_memory         DXMem instance to get direct access to the memory
     * @param p_own_address    where the bucket is stored which will split
     * @param p_address        where the new bucket will stored to
     * @param p_hashId         the used hash algorithm by the HashMap
     * @param p_withInitialize indicates if the initialize method will be call to initialize the new chunk
     * @return the raw data format from the new bucket.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    @Nullable
    private static RawData splitBucket(@NotNull final DXMem p_memory, final long p_own_address, final long p_address,
                                       final byte p_hashId, final boolean p_withInitialize) {
        // read information
        int usedBytes = p_memory.rawRead().readInt(p_own_address, USED_BYTES_OFFSET);
        short size = p_memory.rawRead().readShort(p_own_address, SIZE_OFFSET);
        if (size < 1)
            return null;

        // update depth
        short depth = p_memory.rawRead().readShort(p_own_address, DEPTH_OFFSET);
        depth++;
        p_memory.rawWrite().writeShort(p_own_address, DEPTH_OFFSET, depth);

        // Create RawData for new bucket
        RawData rawData = new RawData();
        rawData.m_depth = depth;

        // helper variables
        short length;
        byte[] hash;
        ArrayList<Integer> space = new ArrayList<>(size);
        int tmp_offset;
        boolean space_mode = false;
        int offset = DATA_OFFSET;
        int current_entry = 1;

        while (current_entry <= size) { // run through data
            tmp_offset = offset;

            // read key
            length = p_memory.rawRead().readShort(p_own_address, offset);
            offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_memory.rawRead().read(p_own_address, offset, stored_key);
            offset += length;

            hash = HashFunctions.hash(p_hashId, stored_key); // hash key

            if (ExtendibleHashing.compareBitForDepth(hash, depth)) { // compare depth_bit

                rawData.appendKey(length, stored_key); // add key

                // read value
                length = p_memory.rawRead().readShort(p_own_address, offset);
                offset += LENGTH_BYTES;
                byte[] stored_value = new byte[length];
                p_memory.rawRead().read(p_own_address, offset, stored_value);
                offset += length;
                rawData.appendValue(length, stored_value); // add value

                if (!space_mode) { // activate space mode
                    space.add(tmp_offset);
                    space_mode = true;
                }

            } else {

                offset += LENGTH_BYTES + p_memory.rawRead().readShort(p_own_address, offset); // skip value

                if (space_mode) { // deactivate space mode
                    space.add(tmp_offset);
                    space_mode = false;
                }
            }

            current_entry++;
        }

        // close space
        // if space == 0 than no pair will be removed
        // if space == 1 than the new size will handle this
        int defragmented_offset;
        int copied_size;
        if (space.size() > 1) {

            defragmented_offset = space.get(0);

            for (int i = 0; i < space.size(); i += 2) {
                if (i + 2 < space.size())
                    copied_size = space.get(i + 2) - space.get(i + 1);
                else if (i + 1 < space.size())
                    copied_size = usedBytes - space.get(i + 1);
                else
                    break; // than the last entry was removed and the new size will handle thisSSS

                copy(p_memory, p_own_address, space.get(i + 1), copied_size, defragmented_offset);
                defragmented_offset += copied_size;
            }
        }

        p_memory.rawWrite().writeInt(p_own_address, USED_BYTES_OFFSET, usedBytes - rawData.m_dataBytes); // update used bytes

        p_memory.rawWrite().writeShort(p_own_address, SIZE_OFFSET, (short) (size - rawData.m_size)); // updated size

        if (p_withInitialize) // initialize new bucket
            initialize(p_memory.rawWrite(), p_address, rawData);

        return rawData;
    }

    /**
     * Splits the bucket and the new bucket is gloabl, so the data could not be written to the memory.
     * Important: the allocated new bucket has to be same size than this bucket
     *
     * @param p_memory      DXMem instance to get direct access to the memory
     * @param p_own_address where the bucket is stored which will splitted
     * @param p_hashId      the used hash algorithm by the HashMap.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static RawData splitBucket(final DXMem p_memory, final long p_own_address, final byte p_hashId) {
        return splitBucket(p_memory, p_own_address, -1L, p_hashId, false);
    }

    /**
     * Returns the calculated stored size of a key-value pair when it will be inserted into the bucket.
     *
     * @param p_suspect  a byte array (key)
     * @param p_suspect2 a byte array (value)
     * @return the calculated stored size of a key-value pair.
     */
    @Contract(pure = true)
    static int calcStoredSize(@NotNull final byte[] p_suspect, @NotNull final byte[] p_suspect2) {
        return p_suspect.length + p_suspect2.length + (LENGTH_BYTES * 2);
    }

    /**
     * Copies a range of bytes.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_address where the bucket is stored
     * @param p_from    offset from where the copy will start (include)
     * @param p_size    range which should be copied
     * @param p_to      offset to be copied to (include)
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void copy(@NotNull final DXMem p_memory, final long p_address, final int p_from,
                     final int p_size, final int p_to) {
        assert p_from > p_to;

        //log.debug(String.format("Copy Memory from %d to %d with size = %d", p_from, p_to, p_size));

        byte[] tmp = new byte[p_size];

        p_memory.rawRead().read(p_address, p_from, tmp);
        p_memory.rawWrite().write(p_address, p_to, tmp);
    }

    /**
     * Returns the calculated individual bucket size for given length of key and value.
     *
     * @param p_entries    maximum entries which the bcuket should store
     * @param p_keyBytes   size of a key
     * @param p_valueBytes size of a value
     * @return the calculated individual bucket size for given length of key and value.
     */
    @Contract(pure = true)
    private static int calcIndividualBucketSize(final int p_entries, final short p_keyBytes, final short p_valueBytes) {
        return (LENGTH_BYTES * 2 + p_keyBytes + p_valueBytes) * p_entries;
    }

    /**
     * Returns a representation of this bucket as string.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_cid     of the bucket
     * @param p_address where the bucket is stored
     * @return a representation of this bucket as string
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    @NotNull
    static String toString(final DXMem p_memory, final long p_cid, final long p_address) {
        StringBuilder builder = new StringBuilder();

        // add allocated Size
        int chunk_size = p_memory.size().size(p_cid);
        builder.append("\n*********************************************************************************************\n");
        builder.append("Bucket Chunk Size: " + chunk_size + " Bytes (Not stored in this Bucket) with CID " + ChunkID.toHexString(p_cid) + "\n");

        // add Header
        builder.append("***HEADER***\n");
        int used = p_memory.rawRead().readInt(p_address, USED_BYTES_OFFSET);
        builder.append("Used: " + used + " Bytes\n");
        builder.append("Depth: " + p_memory.rawRead().readShort(p_address, DEPTH_OFFSET) + "\n");
        short size = p_memory.rawRead().readShort(p_address, SIZE_OFFSET);
        builder.append("Stored KV pairs: " + size + "\n");

        // add Data
        builder.append("***DATA***\n");
        int offset = DATA_OFFSET;
        short length;
        byte[] buffer;
        int current = 1;

        while (current <= size) {
            length = p_memory.rawRead().readShort(p_address, offset);
            buffer = new byte[length];
            offset += LENGTH_BYTES;
            p_memory.rawRead().read(p_address, offset, buffer);
            offset += length;
            builder.append("Key: " + Arrays.toString(buffer) + "\n");

            length = p_memory.rawRead().readShort(p_address, offset);
            buffer = new byte[length];
            offset += LENGTH_BYTES;
            p_memory.rawRead().read(p_address, offset, buffer);
            offset += length;
            builder.append("Value: " + Arrays.toString(buffer) + "\n");
            current++;
        }

        // read(past) bytes
        builder.append("***Bytes are read***\n");
        builder.append("with header: " + offset + "\nwithout header: " + (offset - DATA_OFFSET));
        builder.append("\n*********************************************************************************************\n");

        return builder.toString();
    }
}
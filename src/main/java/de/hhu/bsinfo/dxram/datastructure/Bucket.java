package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;
import de.hhu.bsinfo.dxram.datastructure.util.Converter;
import de.hhu.bsinfo.dxram.datastructure.util.ExtendibleHashing;
import de.hhu.bsinfo.dxram.datastructure.util.HashFunctions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class could only be used while the Memory is pinned.
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
 **/
@PinnedMemory
@NoParamCheck
public class Bucket {

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

    static class BucketRawData {

        private static int HEADER_BYTES = Short.BYTES * 2;

        private ByteArrayOutputStream m_byteStream;
        private short m_size;
        private short m_depth;
        private int m_dataBytes;

        /*** Package Private Usage***/

        byte[] getByteArray() {
            appendHeader();
            return m_byteStream.toByteArray();
        }

        void appendKey(final short p_length, final byte[] p_bytes) {
            m_size++;
            append(p_length, p_bytes);
        }

        void appendValue(final short p_length, final byte[] p_bytes) {
            append(p_length, p_bytes);
        }

        boolean isEmpty() {
            return m_size == 0;
        }

        public short getSize() {
            return m_size;
        }

        public short getDepth() {
            return m_depth;
        }

        public int getDataBytes() {
            return m_dataBytes;
        }

        /*** Private Usage ***/
        private BucketRawData() {
            m_byteStream = new ByteArrayOutputStream();
        }

        private void append(final short p_length, final byte[] p_bytes) {
            try {
                m_byteStream.write(Converter.shortToByteArray(p_length));
                m_byteStream.write(p_bytes);
            } catch (IOException p_e) {
                p_e.printStackTrace();
            }
            m_dataBytes += Short.BYTES + p_length;
        }

        private void appendHeader() {
            try {
                m_byteStream.write(Converter.shortToByteArray(m_size));
                m_byteStream.write(Converter.shortToByteArray(m_depth));
            } catch (IOException p_e) {
                p_e.printStackTrace();
            }
        }


        /*** Private Static Usage ***/
        private static void initialize(final RawWrite p_writer, final long p_address, final byte[] p_rawDataBytes) {
            p_writer.writeInt(p_address, USED_BYTES_OFFSET, p_rawDataBytes.length - BucketRawData.HEADER_BYTES + DATA_OFFSET);
            p_writer.writeShort(p_address, DEPTH_OFFSET, extractDepth(p_rawDataBytes));
            p_writer.writeShort(p_address, SIZE_OFFSET, extractSize(p_rawDataBytes));
            p_writer.write(p_address, DATA_OFFSET, p_rawDataBytes, 0, p_rawDataBytes.length - HEADER_BYTES);
        }

        private static short extractDepth(final byte[] p_rawDataBytes) {
            return Converter.byteArrayToShort(p_rawDataBytes, p_rawDataBytes.length - 1);
        }

        private static short extractSize(final byte[] p_rawDataBytes) {
            return Converter.byteArrayToShort(p_rawDataBytes, p_rawDataBytes.length - 3);
        }
    }

    /**
     * @return
     */
    static int getInitialMemorySize() {
        return DATA_OFFSET;
    }

    /**
     * @param p_writer
     * @param p_address
     * @param p_depth
     */
    static void initialize(final RawWrite p_writer, final long p_address, final short p_depth) {
        p_writer.writeInt(p_address, USED_BYTES_OFFSET, DATA_OFFSET);
        p_writer.writeShort(p_address, DEPTH_OFFSET, p_depth);
        p_writer.writeShort(p_address, SIZE_OFFSET, (short) 0);
    }

    /**
     * @param p_writer
     * @param p_address
     * @param p_rawData
     */
    private static void initialize(final RawWrite p_writer, final long p_address, final BucketRawData p_rawData) {
        // write Header
        p_writer.writeInt(p_address, USED_BYTES_OFFSET, DATA_OFFSET + p_rawData.m_dataBytes);
        p_writer.writeShort(p_address, DEPTH_OFFSET, p_rawData.m_depth);
        p_writer.writeShort(p_address, SIZE_OFFSET, p_rawData.m_size);

        // write Data
        if (p_rawData.m_size > 0)
            p_writer.write(p_address, DATA_OFFSET, p_rawData.m_byteStream.toByteArray());
    }


    /**
     * initializer if you send over the network or you only got the byte-array from inner class
     * RawData.
     * It will overwrite the Chunk and will fill it with the data from p_rawDataBytes.
     *
     * @param p_writer
     * @param p_address
     * @param p_rawDataBytes
     */
    static void initialize(final RawWrite p_writer, final long p_address, final byte[] p_rawDataBytes) {
        BucketRawData.initialize(p_writer, p_address, p_rawDataBytes);
    }

    /**
     * @param p_reader
     * @param p_address
     * @return
     */
    static boolean isFull(final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SIZE_OFFSET) == HashMap.BUCKET_ENTRIES;
    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_key
     * @return
     */
    static boolean contains(final RawRead p_reader, final long p_address, final byte[] p_key) {
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        if (size < 1)
            return false;

        // run through data
        int right_offset = DATA_OFFSET;
        int current_entry = 1;

        while (current_entry <= size) {

            // read key
            short length = p_reader.readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_reader.read(p_address, right_offset, stored_key);
            right_offset += length;

            // compare keys
            if (Arrays.equals(stored_key, p_key))
                return true;

            // skip value
            right_offset += LENGTH_BYTES + p_reader.readShort(p_address, right_offset);

            // increment current entry
            current_entry++;
        }

        return false;
    }

    /**
     * @param p_reader
     * @param p_address
     * @return
     */
    static short getDepth(final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, DEPTH_OFFSET);
    }

    /**
     * @param p_reader
     * @param p_address
     * @return
     */
    static short getSize(final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, SIZE_OFFSET);
    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_withoutHeader
     * @return
     */
    static int getUsedBytes(final RawRead p_reader, final long p_address, final boolean p_withoutHeader) {
        return p_reader.readInt(p_address, USED_BYTES_OFFSET) - (p_withoutHeader ? DATA_OFFSET : 0);
    }

    /**
     * @param p_reader
     * @param p_size
     * @param p_cid
     * @param p_address
     * @param p_bytes
     * @return
     */
    static boolean isEnoughSpace(final RawRead p_reader, final Size p_size, final long p_cid, final long p_address, final int p_bytes) {
        // allocated size of the bucket
        int max_size = p_size.size(p_cid);
        // used bytes by the bucket
        int used = p_reader.readInt(p_address, USED_BYTES_OFFSET);

        return (max_size - used) >= p_bytes;
    }

    /**
     * @param p_reader
     * @param p_size
     * @param p_cid
     * @param p_address
     * @param p_bytes
     * @return
     */
    static int sizeForFit(final RawRead p_reader, final Size p_size, final long p_cid, final long p_address, final int p_bytes) {
        // allocated size of the bucket
        int max_size = p_size.size(p_cid);
        // space between max and used
        int space = max_size - p_reader.readInt(p_address, USED_BYTES_OFFSET);

        if (space >= p_bytes)
            return max_size;
        else
            return max_size + (p_bytes - space);
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_address
     * @param p_key
     * @return
     */
    static byte[] remove(final RawRead p_reader, final RawWrite p_writer, final long p_address, final byte[] p_key) {
        // read information
        int usedBytes = p_reader.readInt(p_address, USED_BYTES_OFFSET);
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        assert size > 0;

        // run through data
        int right_offset = DATA_OFFSET;
        int left_offset;
        int current_entry = 1;

        while (current_entry <= size) {

            // bring offset's together
            left_offset = right_offset;

            // read key
            short length = p_reader.readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_reader.read(p_address, right_offset, stored_key);
            right_offset += length;

            // compare keys
            if (Arrays.equals(stored_key, p_key)) {

                // save value for return
                length = p_reader.readShort(p_address, right_offset);
                right_offset += LENGTH_BYTES;
                byte[] stored_value = new byte[length];
                p_reader.read(p_address, right_offset, stored_value);
                right_offset += length;

                // close space
                if (size - 1 > 0)
                    copy(p_reader, p_writer, p_address, right_offset, usedBytes - right_offset, left_offset);

                // update used bytes
                p_writer.writeInt(p_address, USED_BYTES_OFFSET, usedBytes - (right_offset - left_offset));

                // update size
                p_writer.writeShort(p_address, SIZE_OFFSET, (short) (size - 1));

                return stored_value;
            }

            // skip value
            right_offset += LENGTH_BYTES + p_reader.readShort(p_address, right_offset);

            // increment current entry
            current_entry++;
        }

        return null;
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_address
     * @param p_key
     * @param p_value
     * @return
     */
    static boolean remove(final RawRead p_reader, final RawWrite p_writer, final long p_address, final byte[] p_key, final byte[] p_value) {
        // read information
        int usedBytes = p_reader.readInt(p_address, USED_BYTES_OFFSET);
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        assert size > 0;

        // run through data
        int right_offset = DATA_OFFSET;
        int left_offset;
        int current_entry = 1;

        while (current_entry <= size) {
            // bring offset's together
            left_offset = right_offset;

            // read key
            short length = p_reader.readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_reader.read(p_address, right_offset, stored_key);
            right_offset += length;

            // compare keys
            if (Arrays.equals(stored_key, p_key)) {

                length = p_reader.readShort(p_address, right_offset);
                byte[] stored_value = new byte[length];
                right_offset += LENGTH_BYTES;
                p_reader.read(p_address, right_offset, stored_value);
                right_offset += length;

                if (Arrays.equals(stored_value, p_value)) {

                    // close space
                    if (size - 1 > 0)
                        copy(p_reader, p_writer, p_address, right_offset, usedBytes - right_offset, left_offset);

                    // update used bytes
                    p_writer.writeInt(p_address, USED_BYTES_OFFSET, usedBytes - (right_offset - left_offset));

                    // update size
                    p_writer.writeShort(p_address, SIZE_OFFSET, (short) (size - 1));

                    return true;
                }
            }

            // skip value
            right_offset += LENGTH_BYTES + p_reader.readShort(p_address, right_offset);

            // increment current entry
            current_entry++;
        }

        return false;
    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_key
     * @return
     */
    static byte[] get(final RawRead p_reader, final long p_address, final byte[] p_key) {
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        assert size > -1;

        // run through data
        int right_offset = DATA_OFFSET;
        int current_entry = 1;

        while (current_entry <= size) {

            // read key
            short length = p_reader.readShort(p_address, right_offset);
            right_offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_reader.read(p_address, right_offset, stored_key);
            right_offset += length;

            // compare keys
            if (Arrays.equals(stored_key, p_key)) {

                // read value
                length = p_reader.readShort(p_address, right_offset);
                right_offset += LENGTH_BYTES;
                byte[] stored_value = new byte[length];
                p_reader.read(p_address, right_offset, stored_value);

                return stored_value;
            }

            // skip value
            right_offset += LENGTH_BYTES + p_reader.readShort(p_address, right_offset);

            // increment current entry
            current_entry++;
        }

        return null;
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_address
     * @param p_key
     * @param p_value
     */
    static void put(final RawRead p_reader, final RawWrite p_writer, final long p_address, final byte[] p_key, final byte[] p_value) {
        // read information
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        assert size >= 0;

        // run through data
        int offset = DATA_OFFSET;
        int current_entry = 1;

        short length;

        while (current_entry <= size) {
            // skip key
            length = p_reader.readShort(p_address, offset);
            offset += LENGTH_BYTES + length;

            // skip value
            length = p_reader.readShort(p_address, offset);
            offset += LENGTH_BYTES + length;

            // increment current entry
            current_entry++;
        }

        // write key-value pair
        p_writer.writeShort(p_address, offset, (short) p_key.length);
        offset += LENGTH_BYTES;
        p_writer.write(p_address, offset, p_key);
        offset += p_key.length;
        p_writer.writeShort(p_address, offset, (short) p_value.length);
        offset += LENGTH_BYTES;
        p_writer.write(p_address, offset, p_value);


        // update used bytes
        int usedBytes = p_reader.readInt(p_address, USED_BYTES_OFFSET);
        usedBytes += calcStoredSize(p_key, p_value);
        p_writer.writeInt(p_address, USED_BYTES_OFFSET, usedBytes);

        // update size
        p_writer.writeShort(p_address, SIZE_OFFSET, (short) (size + 1));
    }

    /**
     * Important: the allocated new bucket has to be same size than this bucket
     *
     * @param p_reader
     * @param p_writer
     * @param p_own_address
     * @param p_address
     * @param p_hashId
     */
    static void splitBucket(final RawRead p_reader, final RawWrite p_writer, final long p_own_address,
                            final long p_address, final byte p_hashId) {
        splitBucket(p_reader, p_writer, p_own_address, p_address, p_hashId, true);
    }

    private static Bucket.BucketRawData splitBucket(final RawRead p_reader, final RawWrite p_writer,
                                                    final long p_own_address, final long p_address, final byte p_hashId, final boolean p_withInitialize) {
        // read information
        int usedBytes = p_reader.readInt(p_own_address, USED_BYTES_OFFSET);
        short size = p_reader.readShort(p_own_address, SIZE_OFFSET);
        if (size < 1)
            return null;

        // update depth
        short depth = p_reader.readShort(p_own_address, DEPTH_OFFSET);
        depth++;
        p_writer.writeShort(p_own_address, DEPTH_OFFSET, depth);

        // run through data
        int offset = DATA_OFFSET;
        int current_entry = 1;

        // Create BucketRawData
        Bucket.BucketRawData rawData = new BucketRawData();
        rawData.m_depth = depth;

        // helper variables
        short length;
        byte[] hash;
        ArrayList<Integer> space = new ArrayList<>(size);
        int tmp_offset;
        boolean space_mode = false;

        // run through data
        while (current_entry <= size) {
            tmp_offset = offset;

            // read key
            length = p_reader.readShort(p_own_address, offset);
            offset += LENGTH_BYTES;
            byte[] stored_key = new byte[length];
            p_reader.read(p_own_address, offset, stored_key);
            offset += length;

            // has key
            hash = HashFunctions.hash(p_hashId, stored_key);

            // compare depth_bit
            if (ExtendibleHashing.compareBitForDepth(hash, depth)) {

                // add key
                rawData.appendKey(length, stored_key);

                // read value
                length = p_reader.readShort(p_own_address, offset);
                offset += LENGTH_BYTES;
                byte[] stored_value = new byte[length];
                p_reader.read(p_own_address, offset, stored_value);
                offset += length;

                // add value
                rawData.appendValue(length, stored_value);

                // activate space mode
                if (!space_mode) {
                    space.add(tmp_offset);
                    space_mode = true;
                }

            } else {

                // skip value
                offset += LENGTH_BYTES + p_reader.readShort(p_own_address, offset);

                // deactivate space mode
                if (space_mode) {
                    space.add(tmp_offset);
                    space_mode = false;
                }
            }

            // increment current entry
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

                copy(p_reader, p_writer, p_own_address, space.get(i + 1), copied_size, defragmented_offset);
                defragmented_offset += copied_size;
            }
        }

        // update used bytes
        p_writer.writeInt(p_own_address, USED_BYTES_OFFSET, usedBytes - rawData.m_dataBytes);

        // updated size
        p_writer.writeShort(p_own_address, SIZE_OFFSET, (short) (size - rawData.m_size));

        // initialize new bucket
        if (p_withInitialize)
            initialize(p_writer, p_address, rawData);

        return rawData;
    }

    static Bucket.BucketRawData splitBucket(final RawRead p_reader, final RawWrite p_writer,
                                            final long p_own_address, final byte p_hashId) {
        return splitBucket(p_reader, p_writer, p_own_address, -1L, p_hashId, false);
    }

    /**
     * @param p_suspect
     * @param p_suspect2
     * @return
     */
    static int calcStoredSize(final byte[] p_suspect, final byte[] p_suspect2) {
        return p_suspect.length + p_suspect2.length + (LENGTH_BYTES * 2);
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_address
     * @param p_from
     * @param p_size
     * @param p_to
     */
    static void copy(final RawRead p_reader, final RawWrite p_writer, final long p_address, final int p_from,
                     final int p_size, final int p_to) {
        assert p_from > p_to;

        log.debug(String.format("Copy Memory from %d to %d with size = %d", p_from, p_to, p_size));

        byte[] tmp = new byte[p_size];
        p_reader.read(p_address, p_from, tmp);
        p_writer.write(p_address, p_to, tmp);
    }

    /**
     * @param p_size
     * @param p_reader
     * @param p_cid
     * @param p_address
     * @return
     */
    static String toString(final Size p_size, final RawRead p_reader, final long p_cid, final long p_address) {
        StringBuilder builder = new StringBuilder();

        // add allocated Size
        int chunk_size = p_size.size(p_cid);
        builder.append("\n*********************************************************************************************\n");
        builder.append("Bucket Chunk Size: " + chunk_size + " Bytes (Not stored in this Bucket) with CID " + ChunkID.toHexString(p_cid) + "\n");

        // add Header
        builder.append("***HEADER***\n");
        int used = p_reader.readInt(p_address, USED_BYTES_OFFSET);
        builder.append("Used: " + used + " Bytes\n");
        builder.append("Depth: " + p_reader.readShort(p_address, DEPTH_OFFSET) + "\n");
        short size = p_reader.readShort(p_address, SIZE_OFFSET);
        builder.append("Stored KV pairs: " + size + "\n");

        // add Data
        builder.append("***DATA***\n");
        int offset = DATA_OFFSET;
        short length;
        byte[] buffer;
        int current = 1;

        while (current <= size) {
            length = p_reader.readShort(p_address, offset);
            buffer = new byte[length];
            offset += LENGTH_BYTES;
            p_reader.read(p_address, offset, buffer);
            offset += length;
            builder.append("Key: " + Arrays.toString(buffer) + "\n");

            length = p_reader.readShort(p_address, offset);
            buffer = new byte[length];
            offset += LENGTH_BYTES;
            p_reader.read(p_address, offset, buffer);
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

    /**
     * @param p_entries
     * @param p_keyBytes
     * @param p_valueBytes
     * @return
     */
    static int calcIndividualBucketSize(final int p_entries, final short p_keyBytes, final short p_valueBytes) {
        return (LENGTH_BYTES * 2 + p_keyBytes + p_valueBytes) * p_entries;
    }
}
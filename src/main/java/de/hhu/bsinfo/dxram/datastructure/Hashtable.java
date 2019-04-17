package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;

import java.util.HashSet;

/**
 * This class could only be used while the Memory is pinned.
 * <p>
 * Memory
 * +---------------------------+
 * | 2 Bytes depth :: (bytes)* |
 * +---------------------------+
 * <p>
 **/
@PinnedMemory
@NoParamCheck
public class Hashtable {

    private static final int DEPTH_OFFSET;
    private static final int DATA_OFFSET;

    static {
        DEPTH_OFFSET = 0;
        DATA_OFFSET = Short.BYTES;
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
     * @param p_depth
     * @return
     */
    static int getInitialMemorySize(final short p_depth) {
        return DATA_OFFSET + calcTableSize(p_depth);
    }

    /**
     * @param p_depth
     * @return
     */
    private static int calcTableSize(final short p_depth) {
        return (int) Math.pow(2, p_depth) * Long.BYTES;
    }

    /**
     * @param p_writer
     * @param p_size
     * @param p_address
     * @param p_depth
     * @param p_value
     */
    static void initialize(final RawWrite p_writer, final long p_address, final int p_size, final int p_depth, final long p_value) {
        p_writer.writeInt(p_address, DEPTH_OFFSET, p_depth);

        for (int i = DATA_OFFSET; i < p_size; i += Long.BYTES) {
            p_writer.writeLong(p_address, i, p_value);
        }
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_resize
     * @param p_pinning
     * @param p_cid
     * @param p_address
     * @return
     */
    static long resize(final RawRead p_reader, final RawWrite p_writer, final Resize p_resize, final Pinning p_pinning, final long p_cid, long p_address) {
        // increment depth
        short depth = (short) (p_reader.readShort(p_address, DEPTH_OFFSET) + 1);
        p_writer.writeShort(p_address, DEPTH_OFFSET, depth);

        // resize chunk
        int newSize = (int) Math.pow(2, depth) * Long.BYTES + DATA_OFFSET;
        p_pinning.unpin(p_cid);
        p_resize.resize(p_cid, newSize);
        p_address = p_pinning.pin(p_cid).getAddress(); // maybe check if error for resize

        // get table sizes
        int current_size = newSize - DATA_OFFSET;
        int old_size = current_size / 2;

        // set offset's
        int left_offset = DATA_OFFSET + old_size - Long.BYTES;
        int right_offset = DATA_OFFSET + current_size - Long.BYTES;

        // run through old size from max offset to 0
        while (left_offset >= DATA_OFFSET) {
            long val = p_reader.readLong(p_address, left_offset);

            p_writer.writeLong(p_address, right_offset, val);
            right_offset -= Long.BYTES;
            p_writer.writeLong(p_address, right_offset, val);

            left_offset -= Long.BYTES;
            right_offset -= Long.BYTES;
        }

        return p_address;
    }

    /**
     * @param p_reader
     * @param p_writer
     * @param p_size
     * @param p_cid
     * @param p_address
     * @param p_suspect
     * @param p_position
     * @param p_value
     */
    static void splitForEntry(final RawRead p_reader, final RawWrite p_writer, final Size p_size, final long p_cid,
                              final long p_address, final long p_suspect, final int p_position, final long p_value) {
        int table_size = p_size.size(p_cid) - DATA_OFFSET;

        int position = p_position * Long.BYTES;

        int left_offset = getOffsetDownwards(p_reader, p_address, p_suspect, position); // included
        int right_offset = getOffsetUpwards(p_reader, p_address, table_size, p_suspect, position); // excluded

        int start = DATA_OFFSET + left_offset + ((right_offset - left_offset) / 2);
        int end = DATA_OFFSET + right_offset;

        for (int i = start; i < end; i += Long.BYTES) {
            p_writer.writeLong(p_address, i, p_value);
        }
    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_suspect
     * @param p_position
     * @return
     */
    private static int getOffsetDownwards(final RawRead p_reader, final long p_address, final long p_suspect, final int p_position) {
        int offset = DATA_OFFSET + p_position - Long.BYTES;

        while (offset >= DATA_OFFSET) {

            long entry = p_reader.readLong(p_address, offset);

            if (entry != p_suspect) {
                return offset + Long.BYTES - DATA_OFFSET;
            }

            offset -= Long.BYTES;
        }

        return 0;

    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_size
     * @param p_suspect
     * @param p_position
     * @return excluded index where the first entry != p_suspect is or if last entry == p_suspect it will returned p_size
     */
    private static int getOffsetUpwards(final RawRead p_reader, final long p_address, final int p_size, final long p_suspect, final int p_position) {
        int offset = DATA_OFFSET + p_position + Long.BYTES;

        int end = p_size + DATA_OFFSET;
        while (offset < end) {
            long entry = p_reader.readLong(p_address, offset);

            if (entry != p_suspect) {
                return offset - DATA_OFFSET;
            }

            offset += Long.BYTES;
        }

        return p_size;
    }

    /**
     * @param p_reader
     * @param p_address
     * @param p_position
     * @return
     */
    static long lookup(final RawRead p_reader, final long p_address, final int p_position) {
        return p_reader.readLong(p_address, DATA_OFFSET + (p_position * Long.BYTES));
    }

    static HashSet<Long> bucketCIDs(final Size p_size, final RawRead p_reader, final long p_cid, final long p_address) {
        // TODO: Multithreaded -> 4 Threads
        short depth = p_reader.readShort(p_address, DEPTH_OFFSET);
        int size = p_size.size(p_cid);
        HashSet<Long> set = new HashSet<>((int) Math.pow(2, depth)); // max size 2^depth

        // run through hashtable
        int offset = DATA_OFFSET;
        long read_cid;
        while (offset < size) {

            read_cid = p_reader.readLong(p_address, offset);

            if (!set.contains(read_cid)) {
                set.add(read_cid);
            }

            offset += Long.BYTES;
        }

        return set;
    }

    static void clear(final Size p_size, final RawWrite p_writer, final long p_cid, final long p_address, final long p_defaultEntry) {
        int size = p_size.size(p_cid);
        int offset = DATA_OFFSET;

        while (offset < size) {

            // overwrite all entries
            p_writer.writeLong(p_address, offset, p_defaultEntry);

            offset += Long.BYTES;
        }
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
        builder.append("Hashtable Chunk Size: " + chunk_size + " Bytes\n");

        // add Header
        builder.append("***HEADER***\n");
        builder.append("Depth: " + p_reader.readShort(p_address, DEPTH_OFFSET) + "\n");

        // add Data
        builder.append("***DATA***\n");
        int offset = DATA_OFFSET;
        while (offset < chunk_size) {
            builder.append("0x" + Long.toHexString(p_reader.readLong(p_address, offset)).toUpperCase() + " <== " + offset + "\n");
            offset += Long.BYTES;
        }
        builder.append("\n*********************************************************************************************\n");

        return builder.toString();
    }

}

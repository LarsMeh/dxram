package de.hhu.bsinfo.dxram.datastructure;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxmem.operations.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * This class could only be used while the Memory is pinned. It represents the memory layout from a Hashtable.
 * <p>
 * Memory
 * +---------------------------+
 * | 2 Bytes depth :: (long)*  |
 * +---------------------------+
 * <p>
 *
 * @see de.hhu.bsinfo.dxmem.operations.RawWrite
 * @see de.hhu.bsinfo.dxmem.operations.RawRead
 * @see de.hhu.bsinfo.dxmem.operations.Size
 * @see de.hhu.bsinfo.dxmem.operations.Resize
 * @see de.hhu.bsinfo.dxmem.operations.Pinning
 **/
@PinnedMemory
@NoParamCheck
public class Hashtable {

    private static final Logger log = LogManager.getFormatterLogger(Hashtable.class);

    private static final int DEPTH_OFFSET;
    private static final int DATA_OFFSET;

    static {
        DEPTH_OFFSET = 0;
        DATA_OFFSET = Short.BYTES;
    }

    /**
     * Returns the depth of the Hashtable
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address where the Hashtable is stored.
     * @return the depth of the Hashtable.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static short getDepth(@NotNull final RawRead p_reader, final long p_address) {
        return p_reader.readShort(p_address, DEPTH_OFFSET);
    }

    /**
     * Returns the calculated memory size for this memory layout for a given depth.
     *
     * @param p_depth of the Hashtable.
     * @return the calculated memory size for this memory layout for a given depth.
     */
    @Contract(pure = true)
    public static int getInitialMemorySize(final short p_depth) {
        return DATA_OFFSET + calcTableSize(p_depth);
    }

    /**
     * Returns the calculated memory size of the Hashtable without the header fields.
     *
     * @param p_depth of the Hashtable.
     * @return the calculated memory size of the Hashtable without the header fields.
     */
    @Contract(pure = true)
    private static int calcTableSize(final short p_depth) {
        return (int) Math.pow(2, p_depth) * Long.BYTES;
    }

    /**
     * Returns the calculated depth for the Hashtable based on a value which represents the initial capacity of a datastructure.
     *
     * @param p_value the initial capacity of this HashMap.
     * @return the calculated depth for the Hashtable
     */
    @Contract(pure = true)
    static short calcTableDepth(int p_value, final int p_maxDepth) {
        assert p_value >= 0;

        if (p_value == 0)
            return 1;

        int highestExponent = p_maxDepth + 1; // + 1 generates space

        do {
            p_value = p_value << 1;
            highestExponent--;
        } while (p_value > 0);

        return (short) highestExponent;

    }

    /**
     * Initializes the Hashtable with a given depth. It distributed evenly the values on the allocated size.
     *
     * @param p_writer  DXMem writer for direct memory access.
     * @param p_size    DXMem size-operation.
     * @param p_address where the Hashtable is stored.
     * @param p_depth   of the Hashtable.
     * @param p_value   which will be written to the Hashtable
     * @see de.hhu.bsinfo.dxmem.operations.Size
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    public static void initialize(@NotNull final RawWrite p_writer, final long p_address, final int p_size, final int p_depth, @NotNull final long[] p_value) {
        final int l = p_value.length;
        assert assertInitialEntries(p_depth, l) && p_size >= Math.pow(2, p_depth) + DATA_OFFSET;

        p_writer.writeInt(p_address, DEPTH_OFFSET, p_depth);

        int interval = (p_size - DATA_OFFSET) / l;

        int offset = DATA_OFFSET;
        for (long val : p_value) {

            for (int j = 0; j < interval; j += Long.BYTES) {

                p_writer.writeLong(p_address, offset + j, val);
            }

            offset += interval;
        }
    }

    /**
     * Resize the Hashtable by incrementing the depth which means the size will be doubled.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_cid     ChunkID of the Hashtable.
     * @param p_address where the Hashtable is stored.
     * @return the new address where the Hashtable ist stored.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.Pinning
     * @see de.hhu.bsinfo.dxmem.operations.Resize
     */
    static long resize(@NotNull final DXMem p_memory, final long p_cid, long p_address) {
        // increment depth
        short depth = (short) (p_memory.rawRead().readShort(p_address, DEPTH_OFFSET) + 1);
        p_memory.rawWrite().writeShort(p_address, DEPTH_OFFSET, depth);

        //log.warn("Now Depth is " + depth + " Hash table will be resized from " + ((long) Math.pow(2, depth - 1) * Long.BYTES + DATA_OFFSET) + " to " + ((long) Math.pow(2, depth) * Long.BYTES + DATA_OFFSET));

        // resize chunk
        int newSize = (int) Math.pow(2, depth) * Long.BYTES + DATA_OFFSET;
        p_memory.pinning().unpin(p_cid);
        if (p_memory.resize().resize(p_cid, newSize) != ChunkState.OK)
            throw new RuntimeException("ChunkState for resize call on hashtable is not OK");

        p_address = p_memory.pinning().pin(p_cid).getAddress();


        // get table sizes
        int current_size = newSize - DATA_OFFSET;
        int old_size = current_size / 2;


        // set offset's
        int left_offset = DATA_OFFSET + old_size - Long.BYTES;
        int right_offset = DATA_OFFSET + current_size - Long.BYTES;


        // run through old size from max offset to 0
        while (left_offset >= DATA_OFFSET) {

            long val = p_memory.rawRead().readLong(p_address, left_offset);

            p_memory.rawWrite().writeLong(p_address, right_offset, val);
            right_offset -= Long.BYTES;
            p_memory.rawWrite().writeLong(p_address, right_offset, val);

            left_offset -= Long.BYTES;
            right_offset -= Long.BYTES;
        }

        return p_address;
    }

    /**
     * This method splits the an entry range into a two pieces.
     *
     * @param p_memory   DXMem instance to get direct access to the memory
     * @param p_cid      of the Hashtable.
     * @param p_address  where the Hashtable is stored.
     * @param p_suspect  entry which will be split.
     * @param p_position where the suspect is definitely.
     * @param p_value    new entry.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    public static void splitForEntry(@NotNull final DXMem p_memory, final long p_cid, final long p_address, final long p_suspect,
                                     final int p_position, final long p_value) {
        int memorySize = p_memory.size().size(p_cid);

        int position = p_position * Long.BYTES; // position into position/offset for direct memory access

        int left_offset = getOffsetDownwards(p_memory.rawRead(), p_address, p_suspect, position); // included
        int right_offset = getOffsetUpwards(p_memory.rawRead(), p_address, memorySize, p_suspect, position); // excluded

        int start = DATA_OFFSET + left_offset + ((right_offset - left_offset) / 2); // split the range and take the higher
        int end = DATA_OFFSET + right_offset;

        for (int i = start; i < end; i += Long.BYTES) { // write new entry to higher splitted range
            p_memory.rawWrite().writeLong(p_address, i, p_value);
        }
    }

    /**
     * Returns the determined the left offset from a position downwards.
     *
     * @param p_reader   DXMem reader for direct memory access.
     * @param p_address  where the Hashtable is stored.
     * @param p_suspect  entry which will be splitted.
     * @param p_position where the suspect is definitly.
     * @return the determined the left offset from a position downwards.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    private static int getOffsetDownwards(final RawRead p_reader, final long p_address, final long p_suspect, final int p_position) {
        int offset = DATA_OFFSET + p_position - Long.BYTES;

        while (offset >= DATA_OFFSET) { // run until the lowest offset

            long entry = p_reader.readLong(p_address, offset);

            if (entry != p_suspect) { // break up and return when left offset could be determine
                return offset + Long.BYTES - DATA_OFFSET;
            }

            offset -= Long.BYTES;
        }

        return 0;

    }

    /**
     * Returns the determined the left offset from a position downwards.
     *
     * @param p_reader     DXMem reader for direct memory access.
     * @param p_address    where the Hashtable is stored.
     * @param p_memorySize memory size of the stored Hashtable
     * @param p_suspect    entry which will be splitted.
     * @param p_position   where the suspect is definitly.
     * @return excluded index where the first entry != p_suspect is or if last entry == p_suspect it will returned p_size
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    private static int getOffsetUpwards(final RawRead p_reader, final long p_address, final int p_memorySize, final long p_suspect, final int p_position) {
        int offset = DATA_OFFSET + p_position + Long.BYTES;

        while (offset < p_memorySize) { // run until the highest offset

            long entry = p_reader.readLong(p_address, offset);

            if (entry != p_suspect) { // break up and return when right offset could be determine
                return offset - DATA_OFFSET;
            }

            offset += Long.BYTES;
        }

        return p_memorySize - DATA_OFFSET;
    }

    /**
     * Returns the stored long (ChunkID) which is stored at the given position.
     *
     * @param p_reader   DXMem reader for direct memory access.
     * @param p_address  where the Hashtable is stored.
     * @param p_position which entry should be read from.
     * @return the stored long (ChunkID) which is stored at the given position.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static long lookup(@NotNull final RawRead p_reader, final long p_address, final int p_position) {
        return p_reader.readLong(p_address, DATA_OFFSET + (p_position * Long.BYTES));
    }

    /**
     * Determines a HashSet of all different stored ChunkIDs.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_cid     of the Hashtable.
     * @param p_address where the Hashtable is stored.
     * @return a HashSet of all different stored ChunkIDs.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static HashSet<Long> bucketCIDs(@NotNull final DXMem p_memory, final long p_cid, final long p_address) {
        short depth = p_memory.rawRead().readShort(p_address, DEPTH_OFFSET);
        int size = p_memory.size().size(p_cid);
        HashSet<Long> set = new HashSet<>((int) Math.pow(2, depth)); // max size 2^depth

        int offset = DATA_OFFSET;
        long read_cid;
        while (offset < size) { // run through hashtable

            read_cid = p_memory.rawRead().readLong(p_address, offset);

            if (!set.contains(read_cid)) { // only if set does not contains long
                set.add(read_cid);
            }

            offset += Long.BYTES;
        }

        return set;
    }

    /**
     * Determines a ArrayList of all different stored ChunkIDs in the correct sequence.
     *
     * @param p_memory  DXMem instance to get direct access to the memory
     * @param p_cid     of the Hashtable.
     * @param p_address where the Hashtable is stored.
     * @return an ArrayList of all different stored ChunkIDs in the correct sequence.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static ArrayList<Long> bucketCIDsInSequence(@NotNull final DXMem p_memory, final long p_cid, final long p_address) {
        short depth = p_memory.rawRead().readShort(p_address, DEPTH_OFFSET);
        int size = p_memory.size().size(p_cid);
        HashSet<Long> set = new HashSet<>((int) Math.pow(2, depth)); // max size 2^depth
        ArrayList<Long> list = new ArrayList<>((int) Math.pow(2, depth));

        int offset = DATA_OFFSET;
        long read_cid;
        while (offset < size) { // run through hashtable

            read_cid = p_memory.rawRead().readLong(p_address, offset);

            if (!set.contains(read_cid)) { // only if set does not contains long
                set.add(read_cid);
                list.add(read_cid);
            }

            offset += Long.BYTES;
        }

        return list;
    }

    /**
     * Clears the Hashtable by overwrite all entries with a new default entry.
     *
     * @param p_memory       DXMem instance to get direct access to the memory
     * @param p_cid          of the Hashtable.
     * @param p_address      where the Hashtable is stored.
     * @param p_defaultEntry entry which will be stored as default value.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    static void clear(@NotNull final DXMem p_memory, final long p_cid, final long p_address, final long p_defaultEntry) {
        int size = p_memory.size().size(p_cid);
        int offset = DATA_OFFSET;

        while (offset < size) { // run through the Hashtable

            p_memory.rawWrite().writeLong(p_address, offset, p_defaultEntry); // overwrite with default entry

            offset += Long.BYTES;
        }
    }

    /**
     * Returns a representation of the Hashtable as String.
     *
     * @param p_size    DXMem size-operation.
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_cid     of the Hashtable.
     * @param p_address where the Hashtable is stored.
     * @return a representation of the Hashtable as String.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     * @see de.hhu.bsinfo.dxmem.operations.Size
     */
    @NotNull
    public static String toString(@NotNull final Size p_size, @NotNull final RawRead p_reader, final long p_cid, final long p_address) {
        StringBuilder builder = new StringBuilder();

        // add allocated Size
        int chunk_size = p_size.size(p_cid);
        builder.append("\n*********************************************************************************************\n");
        builder.append("Hashtable Chunk Size: ").append(chunk_size).append(" Bytes\n");

        // add Header
        builder.append("***HEADER***\n");
        builder.append("Depth: ").append(p_reader.readShort(p_address, DEPTH_OFFSET)).append("\n");

        // add Data
        builder.append("***DATA***\n");
        int offset = DATA_OFFSET;
        int position = 0;
        while (offset < chunk_size) {
            builder.append("0x").append(Long.toHexString(p_reader.readLong(p_address, offset)).toUpperCase()).append(" <== ").append(offset).append(" or ").append(position).append("\n");
            offset += Long.BYTES;
            position++;
        }
        builder.append("\n*********************************************************************************************\n");

        return builder.toString();
    }

    /**
     * Asserts if the length are correct for the initial hashtable.
     *
     * @param p_length Number of the initial entries
     * @return true if the number of the initial entries could be represented as 2^x and is not bigger than 2^depth.
     */
    @Contract(pure = true)
    private static boolean assertInitialEntries(final int p_depth, int p_length) {
        if (!(Math.pow(2, p_depth) >= p_length))
            return false;

        while ((p_length & 0x1) == 0) {
            p_length >>= 1;
        }

        p_length >>= 1;

        return p_length == 0;
    }
}

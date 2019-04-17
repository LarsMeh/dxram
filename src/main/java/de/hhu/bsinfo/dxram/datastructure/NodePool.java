package de.hhu.bsinfo.dxram.datastructure;


import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;
import de.hhu.bsinfo.dxutils.NodeID;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

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
public class NodePool {

    private static final int SIZE_OFFSET;
    private static final int DATA_OFFSET;

    static {
        SIZE_OFFSET = 0;
        DATA_OFFSET = 2;
    }

    /**
     * @param p_onlinePeers
     * @param p_numberOfPeers
     * @return
     */
    static int getInitialMemorySize(final int p_onlinePeers, final int p_numberOfPeers) {
        if (p_numberOfPeers <= p_onlinePeers)
            return Short.BYTES * p_numberOfPeers + DATA_OFFSET;
        else {
            // WARN
            return Short.BYTES * p_onlinePeers + DATA_OFFSET;
        }
    }

    /**
     * @param p_writer
     * @param p_onlinePeers
     * @param p_address
     * @param p_numberOfPeers
     */
    static void initialize(final RawWrite p_writer, final List<Short> p_onlinePeers, final long p_address, final int p_numberOfPeers) {
        // Writer Header
        p_writer.writeShort(p_address, SIZE_OFFSET, (short) (p_numberOfPeers + Short.MIN_VALUE));

        Random random = new Random();
        HashSet<Integer> set = new HashSet<>(p_numberOfPeers);

        do {
            set.add(random.nextInt(p_onlinePeers.size()));
        } while (set.size() < p_numberOfPeers);

        int offset = DATA_OFFSET;
        for (int i : set) {
            p_writer.writeShort(p_address, offset, p_onlinePeers.get(i));
            offset += Short.BYTES;
        }
    }

    /**
     * @param p_reader
     * @param p_address
     * @return
     */
    static short getRandomNode(final RawRead p_reader, final long p_address) {
        int offset = ((int) (Math.random() * (p_reader.readShort(p_address, SIZE_OFFSET) - Short.MIN_VALUE)) * Short.BYTES) + DATA_OFFSET;
        return p_reader.readShort(p_address, offset);
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
        builder.append("NodePool Chunk Size: " + chunk_size + " Bytes\n");

        // add Header
        builder.append("***HEADER***\n");
        builder.append("Size: " + ((int) p_reader.readShort(p_address, SIZE_OFFSET) - Short.MIN_VALUE) + "\n");

        // add Data
        builder.append("***DATA***\n");
        int offset = DATA_OFFSET;
        while (offset < chunk_size) {
            builder.append("0x" + NodeID.toHexString(p_reader.readShort(p_address, offset)) + "\n");
            offset += Short.BYTES;
        }
        builder.append("*********************************************************************************************\n");

        return builder.toString().trim();
    }

}

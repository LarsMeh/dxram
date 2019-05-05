package de.hhu.bsinfo.dxram.datastructure;


import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.operations.*;
import de.hhu.bsinfo.dxutils.NodeID;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * This class could only be used while the Memory is pinned. It represents a Pool of NodeIDs.
 * The user could get a random NodeID from this pool.
 * <p>
 * Memory
 * +------------+
 * | (2 bytes)* |
 * +------------+
 * <p>
 *
 * @see de.hhu.bsinfo.dxmem.operations.RawWrite
 * @see de.hhu.bsinfo.dxmem.operations.RawRead
 * @see de.hhu.bsinfo.dxmem.operations.Size
 **/
@PinnedMemory
@NoParamCheck
class NodePool {

    private static final int SIZE_OFFSET;
    private static final int DATA_OFFSET;
    private static final Random RANDOM;

    static {
        SIZE_OFFSET = 0;
        DATA_OFFSET = 2;
        RANDOM = new Random();
    }

    /**
     * Calculates the memory size of this NodePool from a given list and a number of nodes which should be selected.
     *
     * @param p_onlinePeers   list of all online peers.
     * @param p_numberOfPeers which should be randomly selected from the list. Default value -1 means, that all online peers will be selected.
     * @return the calculated memory size.
     */
    static int getInitialMemorySize(final int p_onlinePeers, final int p_numberOfPeers) {
        if (p_numberOfPeers <= p_onlinePeers && p_numberOfPeers != -1)
            return Short.BYTES * p_numberOfPeers + DATA_OFFSET;
        else {
            // WARN
            return Short.BYTES * p_onlinePeers + DATA_OFFSET;
        }
    }

    /**
     * Initializes the NodePool with a number of nodes from a given list. The list contains all online peers.
     *
     * @param p_writer        DXMem writer for direct memory access.
     * @param p_onlinePeers   list of all online peers.
     * @param p_address       address where the NodePool is stored.
     * @param p_numberOfPeers which should be randomly selected from the list. Default value -1 means, that all online peers will be selected.
     * @see de.hhu.bsinfo.dxmem.operations.RawWrite
     */
    static void initialize(final RawWrite p_writer, final List<Short> p_onlinePeers, final long p_address, int p_numberOfPeers) {
        if (p_numberOfPeers == -1) { // default value means that all online peers will be used

            p_numberOfPeers = p_onlinePeers.size();

            int offset = DATA_OFFSET;
            for (short nodeId : p_onlinePeers) { // write all NodeIDs from the list into the memory
                p_writer.writeShort(p_address, offset, nodeId);
                offset += Short.BYTES;
            }

        } else { // select random and different NodeIDs from onlinePeers

            HashSet<Integer> set = new HashSet<>(p_numberOfPeers);

            do {
                set.add(RANDOM.nextInt(p_onlinePeers.size()));
            } while (set.size() < p_numberOfPeers);

            int offset = DATA_OFFSET;
            for (int i : set) {
                p_writer.writeShort(p_address, offset, p_onlinePeers.get(i));
                offset += Short.BYTES;
            }
        }

        p_writer.writeShort(p_address, SIZE_OFFSET, (short) (p_numberOfPeers + Short.MIN_VALUE)); // write header
    }

    /**
     * Returns a random NodeID from the NodePool.
     *
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_address address where the NodePool is stored.
     * @return a random NodeID from the NodePool.
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
     */
    static short getRandomNode(final RawRead p_reader, final long p_address) {
        int offset = ((int) (Math.random() * (p_reader.readShort(p_address, SIZE_OFFSET) - Short.MIN_VALUE)) * Short.BYTES) + DATA_OFFSET;
        return p_reader.readShort(p_address, offset);
    }

    /**
     * Returns a represenation of this NodePool as String.
     *
     * @param p_size    DXMem size-operation.
     * @param p_reader  DXMem reader for direct memory access.
     * @param p_cid     ChunkID which identifies the memory block.
     * @param p_address p_address address where the NodePool is stored.
     * @return a represenation of this NodePool as String.
     * @see de.hhu.bsinfo.dxmem.operations.Size
     * @see de.hhu.bsinfo.dxmem.operations.RawRead
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

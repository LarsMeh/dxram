package de.hhu.bsinfo.dxram.datastructure.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ExtendibleHashing {

    private static final Logger log = LogManager.getFormatterLogger(ExtendibleHashing.class);

    public static boolean compareBitForDepth(final byte[] p_value, final int p_depth) {
        if (p_value.length < 1 || p_depth > (p_value.length * 8)) {

            log.error("length or offset not in range: " + p_value.length);

            return false;
        }

        if (p_depth < 1 || p_depth > 32) {

            log.error("Bit which should be compared is invalid");

            return false;
        }

        int indexArray = 3 - (p_depth - 1) / 8;
        int indexByte = 7 - (p_depth - 1) % 8;

        byte bitmask = (byte) (1 << indexByte);
        byte val = p_value[indexArray];

        return (val & bitmask) != 0;
    }

    /**
     * calculates the index for a given hashmap value with extendable Hashing
     *
     * @param p_depth current depth
     * @param p_array hashmap value
     * @return calculated index from the part until the depth from a hashmap
     */
    public static int extendibleHashing(final byte[] p_array, final short p_depth) {
        if (p_depth < 1 || p_depth > 31 || p_depth > (p_array.length * 8)) {

            log.error("Depth is invalid: " + p_depth);

            return -1;
        }

        int bitmask = Integer.MIN_VALUE;
        bitmask = bitmask >> (p_depth - 1);

        int value = ConverterLittleEndian.byteArrayToInt(p_array); // car array length minimal 8

        int result = (value & bitmask);

        result >>>= (32 - p_depth);

        return result;
    }

}

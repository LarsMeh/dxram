package de.hhu.bsinfo.dxram.datastructure.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.security.InvalidParameterException;

/**
 * This class is a representation of the hash algorithm Extendible Hashing.
 */
public final class ExtendibleHashing {

    private static final Logger log = LogManager.getFormatterLogger(ExtendibleHashing.class);

    private static final int BITMASK = Integer.parseInt("10000000", 2);

    /**
     * It compares the value of the first four Bytes from Parameter p_value with a given depth.
     * A depth of 1 means the highest Bit and a depth of 32 the lowest Bit.
     *
     * @param p_value will be compared.
     * @param p_depth indicates the bit which will be compared.
     * @return true if p_value has a 1 at the given depth.
     * @throws InvalidParameterException
     */
    public static boolean compareBitForDepth(@NotNull final byte[] p_value, int p_depth) {
        if (p_value.length < 1 || p_depth > (p_value.length * 8))
            throw new InvalidParameterException("Invalid Value. Minimum 32-Bit.");

        if (p_depth < 1 || p_depth > 32)
            throw new InvalidParameterException("Invalid Depth");

        p_depth--;

        int bitmask = BITMASK >>> (p_depth % 8);
        int val = p_value[3 - ((p_depth - 1) / 8)];

        return (val & bitmask) != 0;
    }

    /**
     * Calculates the index from a given ByteArray after the principe of ExtendibleHashing.
     * This variant interpreted the depth from the highest to the lowest Bit. It means that a depth of 1 will
     * represent as 1000 ... 0000 and a depth of 32 will be represent as 1111 ... 1111.
     * Only 32-Bit will supported. If your depth is invalid it will return -1.
     *
     * @param p_depth indicates the number of Bits (from highest to lowest) which will be relevant for the index.
     * @param p_array is given value from which will the index be extracted.
     * @return Calculated the index from the part until the depth (included).
     */
    public static int extendibleHashing(final byte[] p_array, final short p_depth) {
        if (p_depth < 1 || p_depth > 31 || p_depth > (p_array.length * 8)) {

            log.error("Depth is invalid: " + p_depth);
            try {
                throw new RuntimeException();
            } catch (RuntimeException p_e) {
                p_e.printStackTrace();
            }

            return -1;
        }

        int bitmask = Integer.MIN_VALUE >> (p_depth - 1);
        int value = ConverterLittleEndian.byteArrayToInt(p_array);

        return (value & bitmask) >>> (32 - p_depth);
    }

}

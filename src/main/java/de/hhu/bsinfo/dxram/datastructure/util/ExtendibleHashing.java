package de.hhu.bsinfo.dxram.datastructure.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

public class ExtendibleHashing {

    private static final Logger log = LogManager.getFormatterLogger(ExtendibleHashing.class);

    public static boolean nBitCompare(final byte[] p_value, final int p_offset) {
        //log.info("nBitCompare call (param: byte[])");
        //log.debug("Parameter Offset=" + p_offset + " Value=" + Arrays.toString(p_value));

        if (p_value.length < 1 || p_offset > (p_value.length * 8)) {

            log.error("length or offset not in range: " + p_value.length);

            return false;
        }

        if (p_offset < 1 || p_offset > 32) {

            log.error("Bit which should be compared is invalid");

            return false;
        }

        int indexArray = (p_offset - 1) / 8;
        int indexByte = (p_offset - 1) % 8;

        byte bitmask = (byte) (1 << indexByte);
        byte val = p_value[indexArray];

        //log.debug("Compare --> bitmask: " + String.format("%8s", Integer.toBinaryString(bitmask & 0xFF)).replace(' ', '0') + " and value: " + String.format("%8s", Integer.toBinaryString(val & 0xFF)).replace(' ', '0') + " from Array: " + Arrays.toString(p_value));

        return (val & bitmask) != 0;
    }

    public static boolean compareBitForDepth(final byte[] p_value, final int p_depth) {
        //log.info("nBitCompare call (param: byte[])");
        //log.debug("Parameter Offset=" + p_offset + " Value=" + Arrays.toString(p_value));

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

        //log.debug("Compare --> bitmask: " + String.format("%8s", Integer.toBinaryString(bitmask & 0xFF)).replace(' ', '0') + " and value: " + String.format("%8s", Integer.toBinaryString(val & 0xFF)).replace(' ', '0') + " from Array: " + Arrays.toString(p_value));
        //System.out.println("Compare --> bitmask: " + String.format("%8s", Integer.toBinaryString(bitmask & 0xFF)).replace(' ', '0') + " and value: " + String.format("%8s", Integer.toBinaryString(val & 0xFF)).replace(' ', '0') + " from Array: " + Arrays.toString(p_value));

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
        //log.info("extendibleHashing call");
        //log.debug("Parameter Depth=" + p_depth);

        if (p_depth < 1 || p_depth > 31 || p_depth > (p_array.length * 8)) {

            log.error("Depth is invalid: " + p_depth);

            return -1;
        }

        //System.out.println("Max.Value - 1 = " + Integer.toBinaryString(Integer.MAX_VALUE - 1));
        int bitmask = Integer.MIN_VALUE;
        bitmask = bitmask >> (p_depth - 1);
        //System.out.println("Bitmask: " + Integer.toBinaryString(bitmask));

        int value = Converter.byteArrayToInt(p_array); // car array length minimal 8

        //System.out.println("Value : " + Integer.toBinaryString(value));

        //log.debug("Bitmask= " + Integer.toBinaryString(bitmask));
        //.debug("Value= " + Integer.toBinaryString(value));
        //System.out.println("Bitmask= " + Integer.toBinaryString(bitmask));
        //System.out.println("Value= " + Integer.toBinaryString(value));

        int result = (value & bitmask);

        //log.debug("Result=" + Integer.toBinaryString(result));

        result >>>= (32 - p_depth);

        //log.debug("Shifted Result=" + Integer.toBinaryString(result));
        return result;
    }

}

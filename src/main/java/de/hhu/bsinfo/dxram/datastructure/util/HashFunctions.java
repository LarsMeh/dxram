package de.hhu.bsinfo.dxram.datastructure.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.security.InvalidParameterException;

public class HashFunctions {

    private static final int SEED;

    public static final byte MURMUR3_32 = 1;
    private static final byte MURMUR3_128 = 2;

    private static final HashFunction m_murmur3_32;
    private static final HashFunction m_murmur3_128;

    static {
        SEED = Integer.parseInt("0101 0101 0101 0101".replace(" ", ""), 2);
        m_murmur3_32 = Hashing.murmur3_32(SEED);
        m_murmur3_128 = Hashing.murmur3_128(SEED);
    }

    public static byte[] hash(byte p_id, byte[] p_value) {
        switch (p_id) {
            case MURMUR3_32:
                return m_murmur3_32.hashBytes(p_value).asBytes();
            case MURMUR3_128:
                return m_murmur3_128.hashBytes(p_value).asBytes();
            default:
                throw new InvalidParameterException();
        }
    }

    public static boolean isProperValue(byte p_id) {
        return p_id > 0 && p_id < 3;
    }

    public static String toString(final byte p_id) {
        switch (p_id) {
            case MURMUR3_32:
                return "Murmur32";
            case MURMUR3_128:
                return "Murmur128";
            default:
                return "HashFunction: none";
        }
    }

}
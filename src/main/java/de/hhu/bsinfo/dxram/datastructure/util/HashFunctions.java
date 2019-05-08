package de.hhu.bsinfo.dxram.datastructure.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.HashMap;

/**
 * This class holds hash functions. All hash functions are static.
 */
public class HashFunctions {

    private static final int SEED;

    public static final byte MURMUR3_32 = 1;
    public static final byte MURM = 1;

    private static HashMap<Byte, HashFunction> functions;

    static {
        SEED = Integer.parseInt("0101 0101 0101 0101".replace(" ", ""), 2);
        Hashing.goodFastHash(32);//;
        functions = new HashMap<>(1);
        functions.put(MURMUR3_32, Hashing.murmur3_32(SEED));
        functions.put(MURMUR3_32, Hashing.murmur3_32(SEED));
    }

    /**
     * Hashes a array of bytes with a hash function. The hash function could be selected by the parameter id.
     * If the id is not valid the method will return null. @see {@link java.util.HashMap#get(Object)}
     *
     * @param p_id    indicates the hash function which should be used.
     * @param p_value will be hashed.
     * @return hash value from p_value.
     */
    public static byte[] hash(byte p_id, byte[] p_value) {
        return functions.get(p_id).hashBytes(p_value).asBytes();
    }

    /**
     * Indicates if a hash function exists for parameter p_id.
     *
     * @param p_id should indicates a hash function
     * @return true if a hash function exists for parameter p_id.
     */
    public static boolean isProperValue(byte p_id) {
        return p_id > 0 && p_id < 3;
    }

    /**
     * Prints the name of the hash function from an parameter p_id.
     *
     * @param p_id should indicates a hash function
     * @return name of the hash function which indicates p_id.
     */
    public static String toString(final byte p_id) {
        switch (p_id) {
            case MURMUR3_32:
                return "Murmur32";
            default:
                return "HashFunction: none";
        }
    }

}
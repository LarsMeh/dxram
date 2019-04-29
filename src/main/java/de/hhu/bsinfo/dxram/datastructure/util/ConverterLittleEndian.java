package de.hhu.bsinfo.dxram.datastructure.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public final class ConverterLittleEndian {

    /* Long - byte[]*/
    @Contract(pure = true)
    public static byte[] longToByteArray(long p_value) {
        byte[] arr = new byte[8];
        arr[0] = (byte) (p_value);
        p_value >>>= 8;
        arr[1] = (byte) (p_value);
        p_value >>>= 8;
        arr[2] = (byte) (p_value);
        p_value >>>= 8;
        arr[3] = (byte) (p_value);
        p_value >>>= 8;
        arr[4] = (byte) (p_value);
        p_value >>>= 8;
        arr[5] = (byte) (p_value);
        p_value >>>= 8;
        arr[6] = (byte) (p_value);
        p_value >>>= 8;
        arr[7] = (byte) (p_value);
        return arr;
    }

    @Contract(pure = true)
    public static long byteArrayToLong(byte[] p_arr) {
        if (p_arr.length < 8) throw new IllegalArgumentException();

        return (((long) p_arr[0]) & 0xFF) +
                ((((long) p_arr[1]) & 0xFF) << 8) +
                ((((long) p_arr[2]) & 0xFF) << 16) +
                ((((long) p_arr[3]) & 0xFF) << 24) +
                ((((long) p_arr[4]) & 0xFF) << 32) +
                ((((long) p_arr[5]) & 0xFF) << 40) +
                ((((long) p_arr[6]) & 0xFF) << 48) +
                ((((long) p_arr[7]) & 0xFF) << 56);
    }

    /* Int - byte[] */

    @Contract(pure = true)
    public static byte[] intToByteArray(int l) {
        byte[] arr = new byte[4];
        arr[0] = (byte) (l);
        l >>>= 8;
        arr[1] = (byte) (l);
        l >>>= 8;
        arr[2] = (byte) (l);
        l >>>= 8;
        arr[3] = (byte) (l);
        return arr;
    }

    @Contract(pure = true)
    public static int byteArrayToInt(byte[] p_arr) {
        if (p_arr.length < 4) throw new IllegalArgumentException();

        int maxOffset = p_arr.length - 1;
        return p_arr[maxOffset - 3] & 0xFF |
                (p_arr[maxOffset - 2] & 0xFF) << 8 |
                (p_arr[maxOffset - 1] & 0xFF) << 16 |
                (p_arr[maxOffset] & 0xFF) << 24;
    }

    /* Short - byte[] */

    @Contract(pure = true)
    public static byte[] shortToByteArray(short p_value) {
        byte[] arr = new byte[2];
        arr[0] = (byte) p_value;
        p_value >>>= 8;
        arr[1] = (byte) p_value;
        return arr;
    }

    @Contract(pure = true)
    public static short byteArrayToShort(@NotNull byte[] p_arr) {
        if (p_arr.length < 2) throw new IllegalArgumentException();
        return (short) ((((short) p_arr[0]) & 0xFF) +
                ((((short) p_arr[1]) & 0xFF) << 8));
    }

    @Contract(pure = true)
    public static short byteArrayToShort(@NotNull byte[] p_arr, int p_index) {
        if (p_arr.length < 2 || p_index < 1) throw new IllegalArgumentException();
        return (short) ((((short) p_arr[p_index - 1]) & 0xFF) +
                ((((short) p_arr[p_index]) & 0xFF) << 8));
    }
}

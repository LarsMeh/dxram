package de.hhu.bsinfo.dxram.datastructure.messages;

public class DataStructureMessageTypes {

    /**
     * HashMap
     **/

    /*----HashMap----*/

    // SUB Types
    public static final byte SUBTYPE_PUT_REQ = 1;
    public static final byte SUBTYPE_PUT_RES = 2;

    public static final byte SUBTYPE_ALLOCATE_RES = 3;
    public static final byte SUBTYPE_ALLOCATE_REQ = 4;

    public static final byte SUBTYPE_WRITE_BUCKET_REQ = 5; // use SignalResponse

    public static final byte SUBTYPE_SIGNAL_RES = 6;

    public static final byte SUBTYPE_REMOVE_REQ = 7;
    public static final byte SUBTYPE_REMOVE_RES = 8;

    public static final byte SUBTYPE_REMOVE_WITH_KEY_REQ = 9; // use SignalResponse

    public static final byte SUBTYPE_GET_REQ = 10;
    public static final byte SUBTYPE_GET_RES = 11;

    public static final byte SUBTYPE_CLEAR_MESSAGE = 12;


    // SUB SUB Types
    public static final byte SUBSUBTYPE_ERROR = 20; // also signal usage
    public static final byte SUBSUBTYPE_RESIZE = 21;
    public static final byte SUBSUBTYPE_BUCKETSPLIT = 22;
    public static final byte SUBSUBTYPE_SUCCESS = 23; // also signal usage
    public static final byte SUBSUBTYPE_OVERWRITE = 26; // also signal usage
    public static final byte SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT = 24;
    public static final byte SUBSUBTYPE_OVERWRITE_AND_BUCKETSPLIT = 25;


    /**
     * Hidden constructor
     */
    private DataStructureMessageTypes() {

    }

    public static String toString(final byte p_type) {
        switch (p_type) {
            case SUBTYPE_PUT_REQ:
                return "PUT_REQ";
            case SUBTYPE_SIGNAL_RES:
                return "SIGNAL";
            case SUBTYPE_PUT_RES:
                return "PUT_RES";
            case SUBSUBTYPE_ERROR:
                return "ERROR";
            case SUBSUBTYPE_BUCKETSPLIT:
                return "BUCKETSPLIT";
            case SUBSUBTYPE_RESIZE:
                return "RESIZE";
            case SUBSUBTYPE_SUCCESS:
                return "SUCCESS";
            case SUBSUBTYPE_OVERWRITE:
                return "OVERWRITE";
            case SUBSUBTYPE_SUCCESS_AND_BUCKETSPLIT:
                return "SUCCESS_AND_BUCKETSPLIT";
            case SUBSUBTYPE_OVERWRITE_AND_BUCKETSPLIT:
                return "OVERWRITE_AND_BUCKETSPLIT";
            case SUBTYPE_REMOVE_REQ:
                return "REMOVE_REQ";
            case SUBTYPE_REMOVE_WITH_KEY_REQ:
                return "REMOVE_WITH_KEY_REQ";
            case SUBTYPE_REMOVE_RES:
                return "REMOVE_RES";
            case SUBTYPE_GET_REQ:
                return "GET_REQ";
            case SUBTYPE_GET_RES:
                return "GET_RES";
            case SUBTYPE_CLEAR_MESSAGE:
                return "CLEAR_MESSAGE";
            default:
                return "Invalid Type(" + p_type + ")";
        }
    }

}

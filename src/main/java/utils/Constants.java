package utils;

public class Constants
{
    public static final int SIZE_MAX_BUFFER_LENGTH = 64 * 1024;

    public final static int SIZE_MAX_KEY_LENGTH = 32;
    public final static int SIZE_MAX_VALUE_LENGTH = 10000;

    public final static int SIZE_MAX_STORE_BYTES = 80 * 1024 * 1024;
    public final static int SIZE_MAX_CACHE_BYTES = 8 * 1024 * 1024;

    public final static int KV_PUT = 0x01;
    public final static int KV_GET = 0x02;
    public final static int KV_REMOVE = 0x03;
    public final static int KV_SHUTDOWN = 0x04;
    public final static int KV_WIPE_OUT = 0x05;
    public final static int KV_IS_ALIVE = 0x06;
    public final static int KV_GET_PID = 0x07;
    public final static int KV_GET_MEMBERSHIP_COUNT = 0x08;

    public final static int ERR_NONE = 0x00;
    public final static int ERR_NON_EXISTENT_KEY = 0x01;
    public final static int ERR_OUT_OF_SPACE = 0x02;
    public final static int ERR_TEMPORARY_SYSTEM_OVERLOAD = 0x03;
    public final static int ERR_INTERNAL_FAILURE = 0x04;
    public final static int ERR_UNRECOGNIZED_COMMAND = 0x05;
    public final static int ERR_INVALID_KEY = 0x06;
    public final static int ERR_INVALID_VALUE = 0x07;
}

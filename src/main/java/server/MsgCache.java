package server;

import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static utils.Constants.SIZE_MAX_CACHE_BYTES;

public class MsgCache
{
    private final Map<ByteString, ByteString> map;
    private final LinkedList<ByteString> list;
    private volatile int bytes;

    private MsgCache()
    {
        map = new ConcurrentHashMap<>();
        list = new LinkedList<>();
        bytes = 0;
    }

    public synchronized void put(ByteString key, ByteString val)
    {
        map.put(key, val);
        bytes += key.size() + val.size();
        list.addLast(key);

        while (bytes > SIZE_MAX_CACHE_BYTES) {
            ByteString removedKey = list.removeFirst();
            ByteString removedVal = map.get(removedKey);
            bytes -= removedKey.size() + removedVal.size();
            map.remove(removedKey);
        }
    }

    public boolean containsKey(ByteString key)
    {
        return map.containsKey(key);
    }

    public ByteString get(ByteString key)
    {
        return map.get(key);
    }

    private static class MsgCacheFactory
    {
        private static final MsgCache INSTANCE = new MsgCache();
    }

    public static MsgCache getInstance()
    {
        return MsgCacheFactory.INSTANCE;
    }
}

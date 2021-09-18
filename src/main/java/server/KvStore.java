package server;

import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static utils.Constants.SIZE_MAX_STORE_BYTES;

public class KvStore
{
    private final Map<ByteString, ValVer> map;
    private volatile int bytes;

    private KvStore()
    {
        map = new ConcurrentHashMap<>();
        bytes = 0;
    }

    public synchronized boolean put(ByteString key, ByteString val, int ver)
    {
        if (bytes + key.size() + val.size() > SIZE_MAX_STORE_BYTES) {
            return false;
        }
        ValVer valVer = map.get(key);
        if (valVer != null) {
            bytes -= key.size() + valVer.getVal().size();
        }
        valVer = new ValVer(val, ver);
        map.put(key, valVer);
        bytes += key.size() + val.size();
        return true;
    }

    public ValVer get(ByteString key)
    {
        return map.get(key);
    }

    public synchronized boolean remove(ByteString key)
    {
        ValVer valVer = map.remove(key);
        if (valVer == null) {
            return false;
        }
        bytes -= key.size() + valVer.getVal().size();
        return true;
    }

    public synchronized void wipe()
    {
        map.clear();
        bytes = 0;
    }

    private static class KvStoreFactory
    {
        private final static KvStore INSTANCE = new KvStore();
    }

    public static KvStore getInstance()
    {
        return KvStoreFactory.INSTANCE;
    }

    public class ValVer
    {
        private final ByteString val;
        private final int ver;

        public ValVer(ByteString val, int ver)
        {
            this.val = val;
            this.ver = ver;
        }

        public ByteString getVal()
        {
            return val;
        }

        public int getVer()
        {
            return ver;
        }
    }
}

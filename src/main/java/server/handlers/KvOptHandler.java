package server.handlers;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import protobuf.KeyValueRequest;
import protobuf.KeyValueResponse;
import protobuf.Message;
import server.KvStore;
import server.MsgCache;
import server.reactor.AbstractNioChannel;
import server.reactor.ChannelHandler;
import server.reactor.NioDatagramChannel.DatagramPacket;
import utils.Protocols;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static utils.Constants.ERR_INVALID_KEY;
import static utils.Constants.ERR_INVALID_VALUE;
import static utils.Constants.ERR_NONE;
import static utils.Constants.ERR_NON_EXISTENT_KEY;
import static utils.Constants.ERR_UNRECOGNIZED_COMMAND;
import static utils.Constants.KV_GET;
import static utils.Constants.KV_GET_MEMBERSHIP_COUNT;
import static utils.Constants.KV_IS_ALIVE;
import static utils.Constants.KV_PUT;
import static utils.Constants.KV_REMOVE;
import static utils.Constants.KV_SHUTDOWN;
import static utils.Constants.KV_WIPE_OUT;
import static utils.Constants.SIZE_MAX_KEY_LENGTH;
import static utils.Constants.SIZE_MAX_VALUE_LENGTH;

public class KvOptHandler
        implements ChannelHandler
{
    private final static Logger LOGGER = LoggerFactory.getLogger(KvOptHandler.class);

    private final MsgCache cache;
    private final KvStore store;

    public KvOptHandler()
    {
        cache = MsgCache.getInstance();
        store = KvStore.getInstance();
    }

    @Override
    public void handleChannelRead(AbstractNioChannel channel, Object readObject, SelectionKey key)
    {
        if (readObject instanceof ByteBuffer) {
            LOGGER.info("TCP data received, nothing will be done");
        }
        else if (readObject instanceof DatagramPacket) {
            DatagramPacket request = (DatagramPacket) readObject;
            byte[] data = handleUdpRead(request);
            DatagramPacket response = new DatagramPacket(ByteBuffer.wrap(data));
            response.setReceiver(request.getSender());
            channel.write(response, key);
        }
        else {
            throw new IllegalStateException("Unknown data received");
        }
    }

    private byte[] handleUdpRead(DatagramPacket packet)
    {
        Message.Msg reqMsg = null;
        try {
            reqMsg = Message.Msg.parseFrom(packet.getData());
        }
        catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        if (reqMsg == null) {
            LOGGER.info("Empty request message received");
            return new byte[0];
        }
        ByteString msgId = reqMsg.getMessageID();
        ByteString resPayLoad;
        if (cache.containsKey(msgId)) {
            resPayLoad = cache.get(msgId);
        }
        else {
            KeyValueRequest.KVRequest request = null;
            try {
                request = KeyValueRequest.KVRequest.parseFrom(reqMsg.getPayload());
            }
            catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            if (request == null) {
                return new byte[0];
            }
            KeyValueResponse.KVResponse response = handle(request);
            resPayLoad = response.toByteString();
            cache.put(msgId, resPayLoad);
        }
        Message.Msg resMsg = Protocols.msg(msgId, resPayLoad);
        return resMsg.toByteArray();
    }

    private KeyValueResponse.KVResponse handle(KeyValueRequest.KVRequest request)
    {
        int cmd = request.getCommand();
        ByteString key = request.getKey();
        ByteString val = request.getValue();
        int ver = request.getVersion();

        KeyValueResponse.KVResponse response = null;
        switch (cmd) {
            case KV_PUT:
                response = put(key, val, ver);
                break;
            case KV_GET:
                response = get(key);
                break;
            case KV_REMOVE:
                response = remove(key);
                break;
            case KV_SHUTDOWN:
                // TODO
                break;
            case KV_WIPE_OUT:
                response = wipe();
                break;
            case KV_IS_ALIVE:
                response = isAlive();
                break;
            case KV_GET_MEMBERSHIP_COUNT:
                response = getMembershipCount();
                break;
            default:
                response = Protocols.kvResponse(
                        ERR_UNRECOGNIZED_COMMAND,
                        null,
                        null,
                        null,
                        null,
                        null);
                break;
        }
        return response;
    }

    private KeyValueResponse.KVResponse put(ByteString key, ByteString val, int ver)
    {
        int err = ERR_NONE;
        if (invalidKey(key)) {
            err = ERR_INVALID_KEY;
        }
        else if (invalidValue(val)) {
            err = ERR_INVALID_VALUE;
        }
        else if (!store.put(key, val, ver)) {
            err = ERR_NON_EXISTENT_KEY;
        }
        return Protocols.kvResponse(
                err,
                null,
                null,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse get(ByteString key)
    {
        int err = ERR_NONE;
        ByteString val = null;
        Integer ver = null;
        if (invalidKey(key)) {
            err = ERR_INVALID_KEY;
        }
        else if (store.get(key) == null) {
            err = ERR_NON_EXISTENT_KEY;
        }
        else {
            KvStore.ValVer valVer = store.get(key);
            val = valVer.getVal();
            ver = valVer.getVer();
        }
        return Protocols.kvResponse(
                err,
                ver,
                val,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse remove(ByteString key)
    {
        int err = ERR_NONE;
        if (invalidKey(key)) {
            err = ERR_INVALID_KEY;
        }
        else if (!store.remove(key)) {
            err = ERR_NON_EXISTENT_KEY;
        }
        return Protocols.kvResponse(
                err,
                null,
                null,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse wipe()
    {
        store.wipe();
        return Protocols.kvResponse(
                ERR_NONE,
                null,
                null,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse isAlive()
    {
        // TODO
        return Protocols.kvResponse(
                ERR_NONE,
                null,
                null,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse getPid()
    {
        // TODO
        return Protocols.kvResponse(
                ERR_NONE,
                null,
                null,
                null,
                null,
                null);
    }

    private KeyValueResponse.KVResponse getMembershipCount()
    {
        // TODO
        return Protocols.kvResponse(
                ERR_NONE,
                null,
                null,
                null,
                null,
                null);
    }

    private boolean invalidKey(ByteString key)
    {
        return key.isEmpty() || key.size() > SIZE_MAX_KEY_LENGTH;
    }

    private boolean invalidValue(ByteString val)
    {
        return val.isEmpty() || val.size() > SIZE_MAX_VALUE_LENGTH;
    }
}

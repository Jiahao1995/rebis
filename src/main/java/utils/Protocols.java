package utils;

import com.google.protobuf.ByteString;
import protobuf.KeyValueResponse;
import protobuf.Message;

import java.util.zip.CRC32;

public class Protocols
{
    public static Message.Msg msg(ByteString msgId, ByteString payload)
    {
        CRC32 crc32 = new CRC32();
        crc32.update(msgId.concat(payload).toByteArray());
        long checksum = crc32.getValue();
        return Message.Msg.newBuilder()
                .setMessageID(msgId)
                .setPayload(payload)
                .setCheckSum(checksum)
                .build();
    }

    public static KeyValueResponse.KVResponse kvResponse(
            Integer err,
            Integer ver,
            ByteString val,
            Integer membershipCount,
            Integer pid,
            Integer overloadWaitTime)
    {
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder();
        if (err != null) {
            builder.setErrCode(err);
        }
        if (ver != null) {
            builder.setVersion(ver);
        }
        if (val != null) {
            builder.setValue(val);
        }
        if (membershipCount != null) {
            builder.setMembershipCount(membershipCount);
        }
        if (pid != null) {
            builder.setPid(pid);
        }
        if (overloadWaitTime != null) {
            builder.setOverloadWaitTime(overloadWaitTime);
        }
        return builder.build();
    }
}

package main.java.com.bag.server.nettyhandlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * BAGMessage Encoder for Netty
 */
public class BAGMessageEncoder extends MessageToByteEncoder<BAGMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, BAGMessage msg, ByteBuf out) throws Exception {
        out.writeInt(msg.size);
        out.writeBytes(msg.buffer);
    }
}

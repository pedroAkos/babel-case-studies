package network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class RequestEncoder extends MessageToByteEncoder<RequestMessage> {

  @Override
  protected void encode(ChannelHandlerContext ctx, RequestMessage msg, ByteBuf out) throws Exception {
    out.writeInt(msg.getcId());
    out.writeByte(msg.getRequestType());
    byte[] bytes = msg.getRequestKey().getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.writeBytes(bytes);
    if(msg.getRequestType() == RequestMessage.WRITE){
      out.writeInt(msg.getRequestValue().length);
      out.writeBytes(msg.getRequestValue());
    }
  }
}

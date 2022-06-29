package hyparview.messages;


import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;


import java.net.UnknownHostException;

public class JoinMessage extends ProtoMessage {
    public final static short MSG_CODE = 401;


    public JoinMessage() {
        super(JoinMessage.MSG_CODE);
    }

    @Override
    public String toString() {
        return "JoinMessage{}";
    }

    public static final ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage m, ByteBuf out) {

        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new JoinMessage();
        }
    };
}

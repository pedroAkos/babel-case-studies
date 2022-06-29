package novasys.app;

public class HashMapCtX {
    private byte[] response;

    public HashMapCtX(){
        response = new byte[0];
    }

    public byte[] getResponse() {
        return response;
    }

    public void setResponse(byte[] response) {
        this.response = response;
    }
}

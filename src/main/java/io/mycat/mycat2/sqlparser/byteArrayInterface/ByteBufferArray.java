package io.mycat.mycat2.sqlparser.byteArrayInterface;

import java.nio.ByteBuffer;

public class ByteBufferArray implements ByteArrayInterface {
    ByteBuffer src;
    @Override
    public byte get(int index) {
        return src.get(index);
    }

    @Override
    public int length() {
        return src.remaining();
    }

    @Override
    public void set(int index, byte value) {
        return;
    }

    public ByteBuffer getSrc() {
        return src;
    }

    public void setSrc(ByteBuffer src) {
        this.src = src;
    }
    public ByteBufferArray() {

    }
    public ByteBufferArray(byte[] arg) {
        src = ByteBuffer.wrap(arg);
    }
    public ByteBufferArray(ByteBuffer arg) {
        src = arg;
    }
}

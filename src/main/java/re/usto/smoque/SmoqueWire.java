package re.usto.smoque;

import re.usto.smoque.io.Mapper;
import re.usto.smoque.util.UnsafeFactory;
import sun.misc.Unsafe;

import java.util.Arrays;


/**
 * Created by tjamir on 9/27/17.
 */
public class SmoqueWire {

    public final static int MAGIC_NUMBER = 0x44A61c;

    public final static int RING_RESET_MAGIC_NUMBER = 0xFA11BACC;

    public static final int PROTOCOL_VERSION = 1;

    //int - 4, long 8
    public static final int HEADER_SIZE = 5*4+8;


    private int magicNumber;

    private int protocolVersion;

    private int totalSize;

    private int producerFieldSize;

    private int payLoadLength;

    private long expiration;

    private byte[] producerId;

    private byte[] payLoad;


    public int getMagicNumber() {
        return magicNumber;
    }

    public SmoqueWire setMagicNumber(int magicNumber) {
        this.magicNumber = magicNumber;
        return this;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public SmoqueWire setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
        return this;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public SmoqueWire setTotalSize(int totalSize) {
        this.totalSize = totalSize;
        return this;
    }

    public int getProducerFieldSize() {
        return producerFieldSize;
    }

    public SmoqueWire setProducerFieldSize(int producerFieldSize) {
        this.producerFieldSize = producerFieldSize;
        return this;
    }

    public int getPayLoadLength() {
        return payLoadLength;
    }

    public SmoqueWire setPayLoadLength(int payLoadLength) {
        this.payLoadLength = payLoadLength;
        return this;
    }

    public long getExpiration() {
        return expiration;
    }

    public SmoqueWire setExpiration(long expiration) {
        this.expiration = expiration;
        return this;
    }

    public byte[] getProducerId() {
        return producerId;
    }

    public SmoqueWire setProducerId(byte[] producerId) {
        this.producerId = producerId;
        return this;
    }

    public byte[] getPayLoad() {
        return payLoad;
    }

    public SmoqueWire setPayLoad(byte[] payLoad) {
        this.payLoad = payLoad;
        return this;
    }

    public static SmoqueWire read(byte[] data) {
        Unsafe unsafe=UnsafeFactory.getUnsafe();
        long offset= Mapper.BYTE_ARRAY_OFFSET;
        SmoqueWire smoqueWire = new SmoqueWire();
        smoqueWire.magicNumber = unsafe.getInt(data, offset);
        offset+=4;
        smoqueWire.protocolVersion = unsafe.getInt(data, offset);
        offset+=4;
        smoqueWire.totalSize=unsafe.getInt(data, offset);
        offset+=4;
        smoqueWire.producerFieldSize=unsafe.getInt(data, offset);
        offset+=4;
        smoqueWire.payLoadLength=unsafe.getInt(data, offset);
        offset+=4;
        smoqueWire.expiration=unsafe.getLong(data, offset);
        offset+=8;
        smoqueWire.producerId=new byte[smoqueWire.producerFieldSize];
        smoqueWire.payLoad=new byte[smoqueWire.payLoadLength];
        unsafe.copyMemory(data, offset, smoqueWire.producerId, Mapper.BYTE_ARRAY_OFFSET, smoqueWire.producerFieldSize);
        offset+=smoqueWire.producerFieldSize;
        unsafe.copyMemory(data, offset, smoqueWire.payLoad, Mapper.BYTE_ARRAY_OFFSET, smoqueWire.payLoadLength);
        return smoqueWire;
    }

    public static byte[] encode(SmoqueWire smoqueWire) {
        byte[] data=new byte[smoqueWire.getTotalSize()];
        Unsafe unsafe=UnsafeFactory.getUnsafe();
        long offset= Mapper.BYTE_ARRAY_OFFSET;
        unsafe.putInt(data, offset, smoqueWire.getMagicNumber());
        offset+=4;
        unsafe.putInt(data, offset, smoqueWire.getProtocolVersion());
        offset+=4;
        unsafe.putInt(data, offset, smoqueWire.getTotalSize());
        offset+=4;
        unsafe.putInt(data, offset, smoqueWire.getProducerFieldSize());
        offset+=4;
        unsafe.putInt(data, offset, smoqueWire.getPayLoadLength());
        offset+=4;
        unsafe.putLong(data, offset, smoqueWire.getExpiration());
        offset+=8;
        unsafe.copyMemory(smoqueWire.getProducerId(), Mapper.BYTE_ARRAY_OFFSET, data, offset,
                smoqueWire.getProducerId().length);
        offset+=smoqueWire.getProducerId().length;
        unsafe.copyMemory(smoqueWire.getPayLoad(), Mapper.BYTE_ARRAY_OFFSET, data, offset,
                smoqueWire.getPayLoad().length);
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SmoqueWire that = (SmoqueWire) o;

        if (magicNumber != that.magicNumber) return false;
        if (protocolVersion != that.protocolVersion) return false;
        if (totalSize != that.totalSize) return false;
        if (producerFieldSize != that.producerFieldSize) return false;
        if (payLoadLength != that.payLoadLength) return false;
        if (expiration != that.expiration) return false;
        if (!Arrays.equals(producerId, that.producerId)) return false;
        return Arrays.equals(payLoad, that.payLoad);
    }

    @Override
    public int hashCode() {
        int result = magicNumber;
        result = 31 * result + protocolVersion;
        result = 31 * result + totalSize;
        result = 31 * result + producerFieldSize;
        result = 31 * result + payLoadLength;
        result = 31 * result + (int) (expiration ^ (expiration >>> 32));
        result = 31 * result + Arrays.hashCode(producerId);
        result = 31 * result + Arrays.hashCode(payLoad);
        return result;
    }

    @Override
    public String toString() {
        return "SmoqueWire{" +
                "magicNumber=" + magicNumber +
                ", protocolVersion=" + protocolVersion +
                ", totalSize=" + totalSize +
                ", producerFieldSize=" + producerFieldSize +
                ", payLoadLength=" + payLoadLength +
                ", expiration=" + expiration +
                ", producerId=" + new String(producerId) +
                '}';
    }
}

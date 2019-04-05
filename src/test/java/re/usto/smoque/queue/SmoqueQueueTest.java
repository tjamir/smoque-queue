package re.usto.smoque.queue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;
import re.usto.smoque.io.Mapper;
import re.usto.smoque.io.MemoryMappedFile;
import re.usto.smoque.util.UnsafeFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by tjamir on 9/26/17.
 */
public class SmoqueQueueTest {


    private SmoqueQueue smoqueQueue;
    private MemoryMappedFile memoryMappedFile;

    private volatile byte[] data;


    @Before
    public void setup() throws IOException {
        data = new byte[QueueHeader.LENGTH];
        SmoqueConfig smoqueConfig = mock(SmoqueConfig.class);
        memoryMappedFile = mockMemoryMappedFile();

        smoqueQueue = new SmoqueQueue(memoryMappedFile, smoqueConfig, "id");
    }


    private MemoryMappedFile mockMemoryMappedFile() throws IOException {
        MemoryMappedFile memoryMappedFile=mock(MemoryMappedFile.class);
        doAnswer(this::writeBlock).when(memoryMappedFile).writeBlock(anyLong(), any());
        when(memoryMappedFile.compareAndSwap(anyLong(), anyLong(), anyLong())).thenAnswer(this::compareAndSwap);
        when(memoryMappedFile.readBlock(anyLong(), anyLong())).thenAnswer(this::readBlock);
        when(memoryMappedFile.getInt( anyLong())).thenAnswer(this::getInt);
        when(memoryMappedFile.getLong(anyLong())).thenAnswer(this::getLong);
        doAnswer(this::putInt).when(memoryMappedFile).putInt(anyLong(), anyInt());
        doAnswer(this::putLong).when(memoryMappedFile).putLong(anyLong(), anyLong());
        return  memoryMappedFile;
    }

    private Object putLong(InvocationOnMock invocationOnMock) {
        long offset=(long)invocationOnMock.getArguments()[0];
        long value=(long)invocationOnMock.getArguments()[1];

        UnsafeFactory.getUnsafe().putLongVolatile(data, offset + Mapper.BYTE_ARRAY_OFFSET, value);
        return  null;
    }

    private Object putInt(InvocationOnMock invocation) {
        long offset=(long)invocation.getArguments()[0];
        int value=(int)invocation.getArguments()[1];

        UnsafeFactory.getUnsafe().putInt(data, offset + Mapper.BYTE_ARRAY_OFFSET, value);
        return  null;

    }

    private byte[] readBlock(InvocationOnMock invocationOnMock) {
        long offset=(long)invocationOnMock.getArguments()[0];
        long length=(long)invocationOnMock.getArguments()[1];
        byte[] readData = new byte[(int)length];
        UnsafeFactory.getUnsafe().copyMemory(data, offset + Mapper.BYTE_ARRAY_OFFSET, readData, Mapper.BYTE_ARRAY_OFFSET, length);
        return readData;
    }

    private int getInt(InvocationOnMock invocationOnMock) {
       long offset=(long)invocationOnMock.getArguments()[0];
       return UnsafeFactory.getUnsafe().getInt(data, Mapper.BYTE_ARRAY_OFFSET+offset);
    }

    private long getLong(InvocationOnMock invocationOnMock) {
        long offset=(long)invocationOnMock.getArguments()[0];
        return UnsafeFactory.getUnsafe().getLong(data, Mapper.BYTE_ARRAY_OFFSET+offset);
    }

    private Object compareAndSwap(InvocationOnMock invocation) {
        //long offset, long expected, long newValue
        long offset = (long) invocation.getArguments()[0];
        long expected = (long) invocation.getArguments()[1];
        long newValue = (long) invocation.getArguments()[2];
        return UnsafeFactory.getUnsafe().compareAndSwapLong(data, offset + Mapper.BYTE_ARRAY_OFFSET, expected, newValue);
    }

    private Object writeBlock(InvocationOnMock invocationOnMock) {
        long offset=(long)invocationOnMock.getArguments()[0];
        byte[] bytes= (byte[]) invocationOnMock.getArguments()[1];
        System.arraycopy(bytes, 0, this.data, (int) offset, bytes.length);
        return null;
    }


    private QueueHeader randomQueueHeader() {
        Random random = new Random(System.currentTimeMillis());
        QueueHeader queueHeader = new QueueHeader();
        queueHeader.setLockOwner(random.nextLong())
                .setFileSize(random.nextLong())
                .setLastMessagePosition(random.nextLong())
                .setLastMessageTimeStamp(random.nextLong())
                .setNextMessagePosition(random.nextLong());
        return queueHeader;
    }

    @Test
    public void putHeader() throws IOException {
        QueueHeader queueHeader = randomQueueHeader();
        smoqueQueue.putQueueHader(queueHeader);
        byte[] data = memoryMappedFile.readBlock(0, QueueHeader.LENGTH);
        QueueHeader queueHeaderRead = new QueueHeader();
        int offset = 0;
        queueHeaderRead.setFileSize(getLong(data, offset));
        offset = offset + 8;
        queueHeaderRead.setLockOwner(getLong(data, offset));
        offset = offset + 8;
        queueHeaderRead.setLastMessageTimeStamp(getLong(data, offset));
        offset = offset + 8;
        queueHeaderRead.setLastMessagePosition(getLong(data, offset));
        offset = offset + 8;
        queueHeaderRead.setNextMessagePosition(getLong(data, offset));

        assertEquals(queueHeader, queueHeaderRead);
    }


    @Test
    public void putReset() throws IOException {
        long offset=67L;
        smoqueQueue.putRingReset(offset);

        assertEquals(SmoqueWire.RING_RESET_MAGIC_NUMBER,getInt(data, offset));
    }


    @Test
    public void testPutGetLong() {
        long value = new Random(System.currentTimeMillis()).nextInt();
        byte[] data = new byte[1 * 1024 * 1024];
        putLong(data, 0, value);
        long read = getLong(data, 0);
        assertEquals(value, read);
    }


    @Test
    public void readHeader() throws IOException {
        QueueHeader queueHeader = randomQueueHeader();
        int offset = 0;
        putLong(data, offset, queueHeader.getFileSize());
        offset += 8;
        putLong(data, offset, queueHeader.getLockOwner());
        offset += 8;
        putLong(data, offset, queueHeader.getLastMessageTimeStamp());
        offset += 8;
        putLong(data, offset, queueHeader.getLastMessagePosition());
        offset += 8;
        putLong(data, offset, queueHeader.getNextMessagePosition());
        QueueHeader queueHeaderRead = smoqueQueue.getQueueHeader();
        assertEquals(queueHeader, queueHeaderRead);
    }

    @Test
    public void lock() throws IOException {
        smoqueQueue.setProcessId(1L);
        boolean locked = smoqueQueue.getExclusiveLock(10000);
        assertTrue(locked);
        assertEquals(1L, getLong(data, QueueHeader.LOCK_OFFSET));
        SmoqueQueue other = new SmoqueQueue(memoryMappedFile, new SmoqueConfig(), "id");
        other.setProcessId(2L);
        locked = other.getExclusiveLock(10000);
        assertFalse(locked);
        assertEquals(1L, getLong(data, QueueHeader.LOCK_OFFSET));
        putLong(data, QueueHeader.LOCK_OFFSET, 0L);
        locked = other.getExclusiveLock(10000);
        assertTrue(locked);
        assertEquals(2L, getLong(data, QueueHeader.LOCK_OFFSET));
        putLong(data, QueueHeader.LOCK_OFFSET, 0L);
    }

    @Test
    public  void release() throws IOException {
        smoqueQueue.setProcessId(1L);
        putLong(data, QueueHeader.LOCK_OFFSET, 0L);
        assertFalse(smoqueQueue.releaseLock());
        assertEquals(0L, getLong(data, QueueHeader.LOCK_OFFSET));
        putLong(data, QueueHeader.LOCK_OFFSET, 1L);
        assertTrue(smoqueQueue.releaseLock());
        assertEquals(0L, getLong(data, QueueHeader.LOCK_OFFSET));
    }

    @Test
    public void readWire() throws IOException, BadFormatException {
        this.data=new byte[2*1024*1024];
        /*
        private int magicNumber;
        private int protocolVersion;
        private int totalSize;
        private int producerFieldSize;
        private int payLoadLength;
        private long expiration;
        private byte[] producerId;
        private byte[] payLoad;
        */

        SmoqueWire smoqueWire = randomSmoqueWire();
        writeWire(smoqueWire, data);

        SmoqueWire readWire=smoqueQueue.readWire(0L);
        assertEquals(smoqueWire, readWire);
    }

    private SmoqueWire randomSmoqueWire() {
        SmoqueWire smoqueWire = new SmoqueWire();
        Random random = new Random();
        byte[] payload=new byte[500*1024];
        random.nextBytes(payload);
        byte[] producer=new byte[1*1024];
        random.nextBytes(producer);
        smoqueWire.setMagicNumber(SmoqueWire.MAGIC_NUMBER);
        smoqueWire.setProtocolVersion(1);
        smoqueWire.setTotalSize(payload.length+producer.length+5*8+64);
        smoqueWire.setProducerFieldSize(producer.length);
        smoqueWire.setPayLoadLength(payload.length);
        smoqueWire.setExpiration(random.nextLong());
        smoqueWire.setProducerId(producer);
        smoqueWire.setPayLoad(payload);
        return smoqueWire;
    }

    private void writeWire(SmoqueWire smoqueWire, byte[] data) {
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

    }


    private void putLong(byte[] data, int offset, long value) {
        Unsafe unsafe = UnsafeFactory.getUnsafe();
        unsafe.putLong(data, (long) offset + Mapper.BYTE_ARRAY_OFFSET, value);
    }

    private long getInt(byte[] data, long offset) {
        Unsafe unsafe = UnsafeFactory.getUnsafe();
        return unsafe.getInt(data,  offset + Mapper.BYTE_ARRAY_OFFSET);
    }

    private long getLong(byte[] data, long offset) {
        Unsafe unsafe = UnsafeFactory.getUnsafe();
        return unsafe.getLong(data,  offset + Mapper.BYTE_ARRAY_OFFSET);
    }

    @Test
    public void putWire() throws IOException {
        this.data=new byte[2*1024*1024];
        SmoqueWire smoqueWire = randomSmoqueWire();
        smoqueQueue.putWire(75L, smoqueWire);
        SmoqueWire readWire = readWire(data, 75L);
        assertEquals(smoqueWire, readWire);
    }

    private SmoqueWire readWire(byte[] data, long offset) {

        Unsafe unsafe=UnsafeFactory.getUnsafe();
        offset= Mapper.BYTE_ARRAY_OFFSET+offset;
        SmoqueWire smoqueWire = new SmoqueWire();
        smoqueWire.setMagicNumber(unsafe.getInt(data, offset));
        offset+=4;
        smoqueWire.setProtocolVersion(unsafe.getInt(data, offset));
        offset+=4;
        smoqueWire.setTotalSize(unsafe.getInt(data, offset));
        offset+=4;
        smoqueWire.setProducerFieldSize(unsafe.getInt(data, offset));
        offset+=4;
        smoqueWire.setPayLoadLength(unsafe.getInt(data, offset));
        offset+=4;
        smoqueWire.setExpiration(unsafe.getLong(data, offset));
        offset+=8;
        smoqueWire.setProducerId(new byte[smoqueWire.getProducerFieldSize()]);
        smoqueWire.setPayLoad(new byte[smoqueWire.getPayLoadLength()]);
        unsafe.copyMemory(data, offset, smoqueWire.getProducerId(), Mapper.BYTE_ARRAY_OFFSET, smoqueWire.getProducerFieldSize());
        offset+= smoqueWire.getProducerFieldSize();
        unsafe.copyMemory(data, offset, smoqueWire.getPayLoad(), Mapper.BYTE_ARRAY_OFFSET, smoqueWire.getPayLoadLength());
        return smoqueWire;

    }

    @Test
    public void readMagicNumber() throws Exception {
        this.putInt(SmoqueWire.MAGIC_NUMBER, 4578L);
        assertEquals(SmoqueWire.MAGIC_NUMBER, smoqueQueue.readMagicNumber(4578L));


    }


    @Test
    public void tryWriteHeader() throws IOException {
        this.data=new byte[QueueHeader.LENGTH];
        QueueHeader queueHeader=randomQueueHeader();
        boolean wrote=smoqueQueue.tryWriteHeader(queueHeader);
        assertTrue(wrote);
        QueueHeader readHeader=smoqueQueue.getQueueHeader();
        assertEquals(queueHeader, readHeader);
    }

    private void putInt(int value, long offset) {
        Unsafe unsafe=UnsafeFactory.getUnsafe();
        unsafe.putInt(this.data, Mapper.BYTE_ARRAY_OFFSET+offset, value);
    }


}
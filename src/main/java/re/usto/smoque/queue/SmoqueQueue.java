package re.usto.smoque.queue;

import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;
import re.usto.smoque.io.Mapper;
import re.usto.smoque.io.MemoryMappedFile;
import re.usto.smoque.util.UnsafeFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by tjamir on 9/20/17.
 */
public class SmoqueQueue {


    private MemoryMappedFile memoryMappedFile;

    private QueueHeader queueHeader;

    private SmoqueConfig smoqueConfig;

    private long processId = 0L;

    private SmoqueTailer tailer;

    private SmoqueAppender appender;

    private String queueId;

    public SmoqueQueue setProcessId(long processId) {
        this.processId = processId;
        return this;
    }

    public long getProcessId() {
        return processId;
    }

    public String getQueueId() {
        return queueId;
    }

    public SmoqueQueue setSmoqueConfig(SmoqueConfig smoqueConfig) {
        this.smoqueConfig = smoqueConfig;
        return this;
    }

    public SmoqueQueue(MemoryMappedFile memoryMappedFile, SmoqueConfig smoqueConfig, String queueId) {
        this.memoryMappedFile = memoryMappedFile;
        this.smoqueConfig = smoqueConfig;
        this.queueId = queueId;
        queueHeader = new QueueHeader();
    }

    public SmoqueConfig getSmoqueConfig() {
        return smoqueConfig;
    }



    public QueueHeader getQueueHeader() throws IOException {
        long position = 0;
        queueHeader.setFileSize(memoryMappedFile.getLong(position));
        position+=8;
        queueHeader.setLockOwner(memoryMappedFile.getLong(position));
        position+=8;
        queueHeader.setLastMessageTimeStamp(memoryMappedFile.getLong(position));
        position+=8;
        queueHeader.setLastMessagePosition(memoryMappedFile.getLong(position));
        position+=8;
        queueHeader.setNextMessagePosition(memoryMappedFile.getLong(position));
        return queueHeader;
    }


    public void putQueueHader(QueueHeader queueHeader) throws IOException {
        long position = Mapper.BYTE_ARRAY_OFFSET;
        byte[] data=new byte[QueueHeader.LENGTH];
        Unsafe unsafe=UnsafeFactory.getUnsafe();

        unsafe.putLong(data, position,  queueHeader.getFileSize());
        position+=8;
        unsafe.putLong(data, position,  queueHeader.getLockOwner());
        position+=8;
        unsafe.putLong(data, position,  queueHeader.getLastMessageTimeStamp());
        position+=8;
        unsafe.putLong(data, position,  queueHeader.getLastMessagePosition());
        position+=8;
        unsafe.putLong(data, position,  queueHeader.getNextMessagePosition());

        memoryMappedFile.writeBlock(0, data);

    }


    public boolean getExclusiveLock(long timeoutNanos) throws IOException {
        long now=System.nanoTime();
        boolean exit=false;
        boolean locked=false;
        while(!exit) {
            locked=memoryMappedFile.compareAndSwap(QueueHeader.LOCK_OFFSET, 0l, processId);
            exit=locked||(timeoutNanos>0&&(System.nanoTime()-now)>timeoutNanos);
            //TODO: sleep
            if(!locked&&!exit&&timeoutNanos>0L){
                UnsafeFactory.getUnsafe().park(false, smoqueConfig.getAcquireIntervalNanos());
            }
        }
        return locked;
    }

    public boolean releaseLock() throws IOException {
        return memoryMappedFile.compareAndSwap(QueueHeader.LOCK_OFFSET, processId, 0L);
    }

    public SmoqueWire readWire(long offset) throws IOException, BadFormatException {
        int magicNumber=memoryMappedFile.getInt(offset);
        if(SmoqueWire.MAGIC_NUMBER!=magicNumber){
                throw new BadFormatException(String.format("Bad magic number: %d!", magicNumber), "magicNumber");
        }
        int protocolVersion = memoryMappedFile.getInt(offset+4);
        if(SmoqueWire.PROTOCOL_VERSION!=protocolVersion){
            throw new BadFormatException("Bad protocol version!", "protocolVersion");
        }
        int totalSize = memoryMappedFile.getInt(offset+8);
        byte[] data = memoryMappedFile.readBlock(offset, (long)totalSize);
        return SmoqueWire.read(data);
    }


    public void putWire(long offset, SmoqueWire smoqueWire) throws IOException {
        byte[] data=SmoqueWire.encode(smoqueWire);
        this.memoryMappedFile.writeBlock(offset, data);
    }


    public void putRingReset(long offset) throws IOException {
        this.memoryMappedFile.putInt(offset, SmoqueWire.RING_RESET_MAGIC_NUMBER);
    }

    public int readMagicNumber(long offset) throws IOException {
        return this.memoryMappedFile.getInt(offset);
    }

    public boolean tryWriteHeader(QueueHeader queueHeader) throws IOException {
        if(!getExclusiveLock(smoqueConfig.getAcquireTimeoutNanos())){
            return false;
        }
        try {
            QueueHeader read = getQueueHeader();
            if (!read.isEmpty()) {
                return false;
            }
            this.putQueueHader(queueHeader);
        }finally {
            releaseLock();
        }
        return true;
    }

    public synchronized SmoqueTailer getTailer() throws IOException {
        if(this.tailer==null){
            tailer=new SmoqueTailer();
            tailer.setPoller(new QueuePoller());
            tailer.setSmoqueQueue(this);
            tailer.start();
        }
        return tailer;

    }

    public synchronized SmoqueAppender getAppender() throws IOException{
        if(this.appender==null){
            appender=new SmoqueAppender();
            appender.setSmoqueConfig(smoqueConfig);
            appender.setSmoqueQueue(this);
        }
        return appender;
    }
}

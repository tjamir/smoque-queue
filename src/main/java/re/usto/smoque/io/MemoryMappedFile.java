package re.usto.smoque.io;

import re.usto.smoque.util.UnsafeFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Created by tjamir on 9/18/17.
 */
public class MemoryMappedFile implements AutoCloseable{

    private long position;

    private long length;

    private Path path;

    public boolean active;

    private Mapper mapper;

    public MemoryMappedFile(Mapper mapper, long length, Path basePath){
        this.mapper =  mapper;
        this.path = basePath;
        this.active = false;
        this.length = length;

    }

    public synchronized void activate() throws IOException {

        if(!active){
            this.position=mapper.map(path, length);
            this.active=true;

        }
    }

    public void passivate() throws IOException {
        if(active){
            mapper.unMap(position, length);
            this.position=-1;
            active = false;
        }

    }

    public boolean isActive() {
        return active;
    }

    public byte[] readBlock(long offset, long length) throws IOException {
        assertActive();

        byte[] data = new byte[(int)length];
        UnsafeFactory.getUnsafe().copyMemory(null, offset+this.position, data, Mapper.BYTE_ARRAY_OFFSET, length);

        return data;
    }

    private void assertActive() throws IOException {
        if(!active){
            throw new IOException("mmap not active for i/o");
        }
    }


    public void writeBlock(long offset, byte [] src) throws IOException {
        assertActive();
        UnsafeFactory.getUnsafe().copyMemory(src, Mapper.BYTE_ARRAY_OFFSET, null, this.position+offset, src.length);

    }

    public boolean compareAndSwap(long offset, long expected, long newValue) throws IOException {
        assertActive();

        return UnsafeFactory.getUnsafe().compareAndSwapLong(null, position+offset, expected, newValue);

    }


    public long getLong(long offset) throws IOException {
        assertActive();
        return UnsafeFactory.getUnsafe().getLongVolatile(null, position+offset);
    }


    public void putLong(long offset, long value) throws IOException {
        assertActive();
        UnsafeFactory.getUnsafe().putLongVolatile(null, position+offset, value);
    }

    public int getInt(long offset) throws IOException {
        assertActive();
        return UnsafeFactory.getUnsafe().getIntVolatile(null, position+offset);
    }

    public void putInt(long offset, int value) throws IOException {
        assertActive();
        UnsafeFactory.getUnsafe().putIntVolatile(null, position+offset, value);
    }

    @Override
    public void close() throws IOException {
        passivate();

    }
}

package re.usto.smoque.io;

import re.usto.smoque.util.UnsafeFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by tjamir on 9/15/17.
 */
public class Mapper {
    private final static Method mapMethod;

    private final static Method unmapMethod;


    public static final int BYTE_ARRAY_OFFSET;


    static {
        try {
            mapMethod = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmapMethod = getMethod(FileChannelImpl.class,"unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = UnsafeFactory.getUnsafe().arrayBaseOffset(byte[].class);

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Method getMethod(Class<?> clazz, String methodName, Class<?>... params) throws Exception {
        Method m = clazz.getDeclaredMethod(methodName, params);
        m.setAccessible(true);
        return m;
    }


    public long map(Path basePath, long size) throws IOException {
        size = roundTo4096(size);

        FileChannel fileChannel = null;
        RandomAccessFile backingFile = null;
        if(Files.exists(basePath)&&Files.size(basePath)!=size){
            throw new IOException("File size and queue size do not match");
        }
        Path absolutePath = basePath.toAbsolutePath();
        if(!Files.exists(absolutePath.getParent())) {
            Files.createDirectories(absolutePath.getParent());
        }
        try {

            backingFile=new RandomAccessFile(basePath.toFile(), "rw");
            backingFile.setLength(size);
            fileChannel=backingFile.getChannel();



            return (long) mapMethod.invoke(fileChannel, 1, 0L, size);
        } catch (Exception e) {
            throw new IOException(e);
        }finally {
            if(fileChannel!=null) {
                fileChannel.close();
            }
            if(backingFile!=null){
                backingFile.close();
            }
        }

    }


    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    public void unMap(long address, long size) throws IOException {
        try {
            size=roundTo4096(size);
            unmapMethod.invoke(null, address, size);
        } catch (IllegalAccessException |InvocationTargetException e) {
            throw new IOException(e);
        }

    }

}




package re.usto.smoque.io;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by tjamir on 9/18/17.
 */
public class MemoryMappedFileTest {


    public static final int LENGTH = 1 * 1024 * 1024;
    public static final Path BASE_PATH = Paths.get("test.bin");
    public static final String HELLO_WORLD = "Hello world!";
    private Mapper mapper;

    @Before
    public void setup() {
        //Impossible to mock this guy, since it allocates memory for direct access. The only way to accomplish this is
        //copying the whole code into test class. I would accept suggestions. (tjamir)
        this.mapper = new Mapper();
    }

    @After
    public void tearDown() throws IOException {
        if(Files.exists(BASE_PATH)){
            Files.delete(BASE_PATH);
        }
    }

    @Test
    public void activateDeactivate() throws IOException {
        MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, LENGTH, BASE_PATH);
        memoryMappedFile.activate();
        assertTrue(memoryMappedFile.isActive());
        memoryMappedFile.passivate();
        assertFalse(memoryMappedFile.isActive());

    }


    @Test
    public void readWriteTest() throws IOException {
        byte[] data = HELLO_WORLD.getBytes();

        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, LENGTH, BASE_PATH)) {
            memoryMappedFile.activate();
            memoryMappedFile.writeBlock(0L, data);
            byte[] read = memoryMappedFile.readBlock(0L, data.length);
            assertNotNull(read);
            assertEquals(HELLO_WORLD, new String(read));
        }
    }

    @Test
    public void compareAndSwap() throws IOException {
        byte[] data=new byte[4096*2];
        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, LENGTH, BASE_PATH)) {
            memoryMappedFile.activate();
            memoryMappedFile.writeBlock(0, data);
            assertTrue(memoryMappedFile.compareAndSwap(37L, 0, 1));
            assertFalse(memoryMappedFile.compareAndSwap(37L, 0, 1));
        }

    }

    @Test
    public void writePerformanceTest() throws IOException {
        byte[] data=new byte[1*1024*1024];
        byte[] zeros=new byte[1*1024*1024];
        Random random=new Random(System.currentTimeMillis());
        random.nextBytes(data);
        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, 1*1024*1024*1024, BASE_PATH)) {
            memoryMappedFile.activate();
            //Warm up
            System.out.println("Warming up");
            final int dataLenth = data.length;
            for(int warm = 0; warm<5; warm++){
                for(int i=0;i<1024;i++){
                    memoryMappedFile.writeBlock(i* dataLenth, zeros);
                }
            }

            System.out.println("Testing");

            int numberOfTests=10;
            List<Long> values = new ArrayList<>();
            for(int test=0;test<numberOfTests;test++){
                long start=System.currentTimeMillis();
                for(int i=0;i<1024;i++){
                    memoryMappedFile.writeBlock(i* dataLenth, data);
                }
                values.add(System.currentTimeMillis()-start);
            }
            long sum=0L;
            long max=0L;
            long min=Long.MAX_VALUE;
            System.out.println("-----------------");
            System.out.println("Write test report");
            System.out.println("-----------------");


            report(values, sum, max, min);


        }
    }

    private void report(List<Long> values, long sum, long max, long min) {
        int run=1;
        for(Long value:values){
            System.out.println(String.format("Run %d ==> Value: %d ms", run, value));
            run++;
            sum=sum+value;
            if(value>max){
                max=value;
            }
            if(value<min){
                min=value;
            }
        }
        System.out.println(String.format("Max value: %d ms", max));
        System.out.println(String.format("Min value: %d ms", min));
        System.out.println(String.format("Total elapsed time: %d ms", sum));
        long average = sum / values.size();
        System.out.println(String.format("Average time: %d", average));
        System.out.println(String.format("Average speed: %d MB/s", 1024*1000/average));
        System.out.println();
    }


    @Test
    public void readPerformanceTest() throws IOException {
        byte[] data=new byte[1*1024*1024];
        Random random=new Random(System.currentTimeMillis());
        random.nextBytes(data);


        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, 1*1024*1024*1024, BASE_PATH)) {
            memoryMappedFile.activate();
            for(int i=0;i<1024;i++){
                memoryMappedFile.writeBlock(i*data.length, data);
            }
            memoryMappedFile.activate();
            //Warm up
            for(int warm=0;warm<5;warm++){
                for(int i=0;i<1024;i++){
                    memoryMappedFile.readBlock(i*data.length, data.length);
                }
            }

            int numberOfTests=10;
            List<Long> values = new ArrayList<>();
            for(int test=0;test<numberOfTests;test++){

                long start=System.currentTimeMillis();
                long dataLength=data.length;
                for(int i=0;i<1024;i++){
                    memoryMappedFile.readBlock(i*data.length, dataLength);
                }
                values.add(System.currentTimeMillis()-start);
            }
            long sum=0L;
            long max=0L;
            long min=Long.MAX_VALUE;
            System.out.println("-----------------");
            System.out.println("Read test report");
            System.out.println("-----------------");

           report(values, sum, max, min);


        }
    }


    @Test
    public void putAndGetLong() throws IOException {

        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, 1*1024*1024*1024, BASE_PATH)) {
            memoryMappedFile.activate();
            memoryMappedFile.putLong(85L, 4567L);
            assertEquals(4567L, memoryMappedFile.getLong(85L));


        }
    }


    @Test
    public void putAndGetInt() throws IOException {
        try (MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, 1*1024*1024*1024, BASE_PATH)) {
            memoryMappedFile.activate();
            memoryMappedFile.putInt(85L, 4567);
            assertEquals(4567, memoryMappedFile.getInt(85L));
        }
    }






}
package re.usto.smoque;

import com.sun.jna.Library;
import com.sun.jna.Native;
import re.usto.smoque.io.Mapper;
import re.usto.smoque.io.MemoryMappedFile;
import re.usto.smoque.queue.QueueHeader;
import re.usto.smoque.queue.SmoqueQueue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tjamir on 9/20/17.
 */
public class Smoque {


    private SmoqueConfig smoqueConfig;
    private Map<String, SmoqueQueue> queueMap;

    private Mapper mapper;

    private long processId;

    private interface CLibrary extends Library {
        CLibrary INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
        int getpid ();
    }




    public Smoque(SmoqueConfig smoqueConfig){
        this.smoqueConfig=smoqueConfig;
        this.queueMap=new ConcurrentHashMap<>();
        this.mapper = new Mapper();
        this.processId = CLibrary.INSTANCE.getpid();

    }

    public long getProcessId() {
        return processId;
    }

    public SmoqueConfig getSmoqueConfig() {
        return smoqueConfig;
    }


    public SmoqueQueue getQueue(String id, long length) throws IOException {

        if(!queueMap.containsKey(id)){
            MemoryMappedFile memoryMappedFile = new MemoryMappedFile(mapper, length, smoqueConfig.getPath().resolve(id));

            SmoqueQueue smoqueQueue=new SmoqueQueue(memoryMappedFile, smoqueConfig, id);
            smoqueQueue.setProcessId(this.processId);
            QueueHeader queueHeader=new QueueHeader().setNextMessagePosition(QueueHeader.LENGTH).setLastMessageTimeStamp(System.currentTimeMillis())
                    .setLastMessagePosition(QueueHeader.LENGTH).setFileSize(length).setLockOwner(smoqueQueue.getProcessId());


            SmoqueQueue previous=queueMap.putIfAbsent(id, smoqueQueue);
            if(previous!=null){
                return previous;
            }
            memoryMappedFile.activate();
            smoqueQueue.tryWriteHeader(queueHeader);

        }

        return queueMap.get(id);
    }



}

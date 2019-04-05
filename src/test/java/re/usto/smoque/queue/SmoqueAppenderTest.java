package re.usto.smoque.queue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Created by tjamir on 9/28/17.
 */
public class SmoqueAppenderTest {


    private SmoqueAppender smoqueAppender;


    private SmoqueQueue smoqueQueue;

    private SmoqueConfig smoqueConfig;

    private SmoqueWire smoqueWire;

    private QueueHeader smoqueHeader;

    private long offset;


    @Before
    public void init() throws IOException {

        initializeLocals();
        createMocks();
        this.smoqueAppender=new SmoqueAppender();
        smoqueAppender.setSmoqueQueue(smoqueQueue);
        smoqueAppender.setSmoqueConfig(smoqueConfig);

    }

    private void initializeLocals(){
        smoqueWire = null;
        offset=0L;
        smoqueHeader = new QueueHeader();
        smoqueHeader.setFileSize(2*1024*1024*1024L);
        smoqueHeader.setNextMessagePosition(1*1024*1024*1024L);
        smoqueHeader.setLastMessageTimeStamp(System.currentTimeMillis());
        smoqueHeader.setLockOwner(0L);
        smoqueHeader.setLastMessagePosition(1*1024*1024*1024-1*1024*1024);
    }

    private void createMocks() throws IOException{

        smoqueQueue=mock(SmoqueQueue.class);
        smoqueConfig=mock(SmoqueConfig.class);
        when(smoqueQueue.getQueueHeader()).thenAnswer(invocation -> smoqueHeader);
        when(smoqueConfig.getAcquireTimeoutNanos()).thenReturn(1*1000*1000L);
        when(smoqueConfig.getMessageTimeoutMs()).thenReturn(5*1000L);
       doAnswer( invocation -> updateHeader(invocation))
               .when(smoqueQueue).putQueueHader(any());
        doAnswer(invocation -> queuePutWire(invocation))
                .when(smoqueQueue).putWire(anyLong(), any());
    }

    private Object updateHeader(InvocationOnMock invocation) {
        QueueHeader queueHeader= (QueueHeader) invocation.getArguments()[0];
        this.smoqueHeader=queueHeader;
        return null;
    }

    private Object queuePutWire(InvocationOnMock invocation) {
        long offset=(long)invocation.getArguments()[0];
        SmoqueWire smoqueWire= (SmoqueWire) invocation.getArguments()[1];
        this.smoqueWire=smoqueWire;
        this.offset = offset;
        return  null;
    }

    @Test
    public void append() throws Exception {
        String producerId = "messageProducer";
        String content = "myMessageDataCanBeBiggerThanThis";

        QueueHeader header=smoqueHeader;
        long expectedPosition=header.getNextMessagePosition();
        when(smoqueQueue.getExclusiveLock(anyLong())).thenReturn(true);
        smoqueAppender.append(producerId, content.getBytes());

        assertNotNull(smoqueWire);

        assertEquals(this.offset, expectedPosition);
        assertTrue(smoqueWire.getExpiration()-System.currentTimeMillis()<=smoqueConfig.getMessageTimeoutMs());
        assertTrue(smoqueWire.getExpiration()>=System.currentTimeMillis());
        assertEquals(SmoqueWire.MAGIC_NUMBER, smoqueWire.getMagicNumber());
        assertEquals(SmoqueWire.PROTOCOL_VERSION, smoqueWire.getProtocolVersion());
        assertEquals(smoqueWire.getPayLoadLength() + smoqueWire.getProducerFieldSize()+5*4+8,
                smoqueWire.getTotalSize());
        assertEquals(smoqueWire.getPayLoadLength(), smoqueWire.getPayLoad().length);
        assertEquals(smoqueWire.getProducerFieldSize(), smoqueWire.getProducerId().length);
        assertTrue(Arrays.equals(smoqueWire.getProducerId(), producerId.getBytes()));
        assertTrue(Arrays.equals(smoqueWire.getPayLoad(), content.getBytes()));

        verify(smoqueQueue).getExclusiveLock(anyLong());

        header = this.smoqueHeader;

        assertEquals(header.getNextMessagePosition(), offset+smoqueWire.getTotalSize());
        verify(smoqueQueue).releaseLock();
    }



    @Test
    public void appendInTheEnd() throws IOException, TimeoutException {
        String producerId = "messageProducer";
        String content = "myMessageDataCanBeBiggerThanThis";
        smoqueHeader.setNextMessagePosition(smoqueHeader.getFileSize()-10L);
        long expectedLastPosition=QueueHeader.LENGTH;
        long expectedNextPostion=expectedLastPosition+producerId.getBytes("utf-8").
                length+content.getBytes().length+SmoqueWire.HEADER_SIZE;
        when(smoqueQueue.getExclusiveLock(anyLong())).thenReturn(true);
        smoqueAppender.append(producerId, content.getBytes());

        assertEquals(expectedLastPosition, this.offset);
        assertEquals(expectedNextPostion, smoqueHeader.getNextMessagePosition());
    }

}
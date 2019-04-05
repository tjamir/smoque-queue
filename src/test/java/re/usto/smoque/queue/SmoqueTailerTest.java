package re.usto.smoque.queue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.verification.VerificationMode;
import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by tjamir on 10/4/17.
 */
public class SmoqueTailerTest {

    private SmoqueTailer smoqueTailer;
    private SmoqueQueue smoqueQueue;
    private SmoqueConfig smoqueConfig;
    private QueuePoller queuePoller;


    private QueueHeader smoqueHeader;

    private Long currentPosition;

    private Long nextPosition;

    @Before
    public void setUp() throws Exception {
        this.currentPosition = 1024 * 1024 + 45L;
        this.smoqueTailer = new SmoqueTailer();
        smoqueTailer.setSmoqueQueue(mockSmoqueQueue());
        smoqueTailer.setPoller(mockPoller());
        nextPosition = currentPosition;
    }

    private SmoqueQueue mockSmoqueQueue() throws IOException {
        this.smoqueQueue = mock(SmoqueQueue.class);
        this.smoqueHeader = mockHeader();
        smoqueQueue.setSmoqueConfig(mockSmoqueConfig());
        when(smoqueQueue.getQueueHeader()).thenReturn(this.smoqueHeader);
        return this.smoqueQueue;

    }

    private QueueHeader mockHeader() {
        this.smoqueHeader = mock(QueueHeader.class);
        doAnswer(this::nextPosition).when(smoqueHeader).getNextMessagePosition();
        when(smoqueHeader.getFileSize()).thenReturn(1*1024*1024*1024L);
        return smoqueHeader;
    }

    private QueuePoller mockPoller() {
        this.queuePoller = mock(QueuePoller.class);
        return queuePoller;
    }

    private Object nextPosition(InvocationOnMock invocationOnMock) {

        return nextPosition;
    }

    private SmoqueConfig mockSmoqueConfig() {
        this.smoqueConfig = mock(SmoqueConfig.class);
        return smoqueConfig;
    }


    @Test
    public void start() throws IOException {
        smoqueTailer.start();
        assertEquals((long) currentPosition, smoqueTailer.currentPosition);
        verify(queuePoller).start(any());
    }


    @Test
    public void onAppend() throws IOException, BadFormatException {
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        when(smoqueWire.getPayLoad()).thenReturn("mypayload".getBytes());
        when(smoqueWire.getProducerId()).thenReturn("producer".getBytes());
        when(smoqueWire.getTotalSize()).thenReturn(60);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        when(smoqueQueue.readMagicNumber(eq(currentPosition))).thenReturn(SmoqueWire.MAGIC_NUMBER);
        smoqueTailer.currentPosition = this.currentPosition;
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(smoqueQueue).readWire(eq(this.currentPosition));

    }


    @Test
    public void onAppendInTheEnd() throws IOException, BadFormatException {
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        byte[] payloadBytes = "mypayload".getBytes();
        when(smoqueWire.getPayLoad()).thenReturn(payloadBytes);
        byte[] producerbytes= "producer".getBytes();
        when(smoqueWire.getProducerId()).thenReturn(producerbytes);
        when(smoqueWire.getTotalSize()).thenReturn(payloadBytes.length+producerbytes.length+SmoqueWire.HEADER_SIZE);
        currentPosition = 1*1024*1024*1024L - 40;
        smoqueTailer.currentPosition = currentPosition;
        when(smoqueQueue.readMagicNumber(eq(currentPosition.longValue()))).thenReturn(SmoqueWire.RING_RESET_MAGIC_NUMBER);
        long queueHeaderLength=QueueHeader.LENGTH;
        nextPosition = queueHeaderLength+smoqueWire.getTotalSize();
        when(smoqueQueue.readMagicNumber(eq(queueHeaderLength))).thenReturn(SmoqueWire.MAGIC_NUMBER);
        when(smoqueQueue.readWire(eq(queueHeaderLength))).thenReturn(smoqueWire);
        when(smoqueQueue.readWire(eq(queueHeaderLength))).thenReturn(smoqueWire);
        smoqueTailer.currentPosition = this.currentPosition;
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(smoqueQueue).readWire(eq(queueHeaderLength));

    }


    @Test
    public void onAppendWrongMagicNumber2() throws IOException, BadFormatException {

        SmoqueConsumer consumer=mock(SmoqueConsumer.class);
        smoqueTailer.addConsumer(consumer);
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        byte[] payload = "invalid content".getBytes();
        byte[] producer = "producer".getBytes();
        when(smoqueWire.getPayLoad()).thenReturn(payload);
        when(smoqueWire.getProducerId()).thenReturn(producer);
        when(smoqueWire.getTotalSize()).thenReturn(payload.length+producer.length+SmoqueWire.HEADER_SIZE);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenThrow(
                new BadFormatException(String.format("Bad magic number: %d!", 0L), "magicNumber"));
        when(smoqueQueue.readMagicNumber(eq(currentPosition))).thenReturn(SmoqueWire.MAGIC_NUMBER);
        smoqueTailer.currentPosition = this.currentPosition;
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        currentPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueWire=mock(SmoqueWire.class);
        payload = "valid content".getBytes();
        producer = "producer".getBytes();
        when(smoqueWire.getPayLoad()).thenReturn(payload);
        when(smoqueWire.getProducerId()).thenReturn(producer);
        when(smoqueWire.getTotalSize()).thenReturn(payload.length+producer.length+SmoqueWire.HEADER_SIZE);
        when(smoqueQueue.readMagicNumber(eq(currentPosition))).thenReturn(SmoqueWire.MAGIC_NUMBER);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(smoqueQueue).readWire(eq(this.currentPosition));
        verify(consumer).onMessage(anyString(), AdditionalMatchers.aryEq(payload));
    }


    @Test
    public void onAppendWrongMagicNumber() throws IOException, BadFormatException {

        SmoqueConsumer consumer=mock(SmoqueConsumer.class);
        smoqueTailer.addConsumer(consumer);
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        byte[] payload = "invalid content".getBytes();
        byte[] producer = "producer".getBytes();
        when(smoqueWire.getPayLoad()).thenReturn(payload);
        when(smoqueWire.getProducerId()).thenReturn(producer);
        when(smoqueWire.getTotalSize()).thenReturn(payload.length+producer.length+SmoqueWire.HEADER_SIZE);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        when(smoqueQueue.readMagicNumber(eq(currentPosition))).thenReturn(0);
        smoqueTailer.currentPosition = this.currentPosition;
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(smoqueQueue, never()).readWire(eq(this.currentPosition));
        currentPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueWire=mock(SmoqueWire.class);
        payload = "valid content".getBytes();
        producer = "producer".getBytes();
        when(smoqueWire.getPayLoad()).thenReturn(payload);
        when(smoqueWire.getProducerId()).thenReturn(producer);
        when(smoqueWire.getTotalSize()).thenReturn(payload.length+producer.length+SmoqueWire.HEADER_SIZE);
        when(smoqueQueue.readMagicNumber(eq(currentPosition))).thenReturn(SmoqueWire.MAGIC_NUMBER);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(smoqueQueue).readWire(eq(this.currentPosition));
        verify(consumer).onMessage(anyString(), AdditionalMatchers.aryEq(payload));
    }


    @Test
    public void addConsumer() throws IOException, BadFormatException {
        SmoqueConsumer consumer = mock(SmoqueConsumer.class);
        smoqueTailer.addConsumer(consumer);
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        when(smoqueWire.getPayLoad()).thenReturn("mypayload".getBytes());
        when(smoqueWire.getProducerId()).thenReturn("producer".getBytes());
        when(smoqueWire.getTotalSize()).thenReturn(60);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        when(smoqueQueue.readMagicNumber(anyLong())).thenReturn(SmoqueWire.MAGIC_NUMBER);
        smoqueTailer.currentPosition = this.currentPosition;
        nextPosition = nextPosition+60;
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(consumer).onMessage(any(), any());

    }




    @Test
    public void removeConsumer() throws IOException, BadFormatException {
        SmoqueConsumer consumer = mock(SmoqueConsumer.class);
        smoqueTailer.addConsumer(consumer);
        SmoqueWire smoqueWire=mock(SmoqueWire.class);
        when(smoqueWire.getPayLoad()).thenReturn("mypayload".getBytes());
        when(smoqueWire.getProducerId()).thenReturn("producer".getBytes());
        when(smoqueWire.getTotalSize()).thenReturn(60);
        when(smoqueQueue.readWire(eq(this.currentPosition))).thenReturn(smoqueWire);
        smoqueTailer.currentPosition = this.currentPosition;
        nextPosition=currentPosition+smoqueWire.getTotalSize();
        when(smoqueQueue.readMagicNumber(anyLong())).thenReturn(SmoqueWire.MAGIC_NUMBER);
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verify(consumer).onMessage(any(), any());
        assertTrue(smoqueTailer.removeConsumer(consumer));
        currentPosition=currentPosition+(60);
        nextPosition=nextPosition+60;
        when(smoqueQueue.readWire(eq(smoqueTailer.currentPosition))).thenReturn(smoqueWire);
        smoqueTailer.onAppend(smoqueQueue.getQueueHeader());
        verifyNoMoreInteractions(consumer);
    }


    @Test
    public void stop() throws IOException, BadFormatException {

        smoqueTailer.stop();
        verify(queuePoller).stop();


    }


}
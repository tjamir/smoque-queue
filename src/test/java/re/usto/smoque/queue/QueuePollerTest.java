package re.usto.smoque.queue;

import org.junit.Before;
import org.junit.Test;
import re.usto.smoque.SmoqueConfig;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by tjamir on 10/11/17.
 */
public class QueuePollerTest {


    private QueuePoller queuePoller;
    private SmoqueTailer smoqueTailer;
    private SmoqueQueue smoqueQueue;
    private QueueHeader queueHeader;
    private SmoqueConfig smoqueConfig;

    private long lastMessageMs;

    private long lastMessagePosition;




    @Before
    public void setUp() throws Exception {
        lastMessageMs = System.currentTimeMillis();
        lastMessagePosition = 45789L;
        queuePoller=new QueuePoller();
        queuePoller.setQueue(mockQueue());
        queuePoller.setTailer(mockTailer());

    }

    private SmoqueTailer mockTailer() {
        smoqueTailer = mock(SmoqueTailer.class);
        return smoqueTailer;
    }

    private SmoqueQueue mockQueue() throws IOException {
        smoqueQueue = mock(SmoqueQueue.class);
        QueueHeader mockHeader = mockHeader();
        when(smoqueQueue.getQueueHeader()).thenReturn(mockHeader);
        when(smoqueQueue.getSmoqueConfig()).thenReturn(createConfig());
        return smoqueQueue;
    }

    private SmoqueConfig createConfig() {
        this.smoqueConfig=new SmoqueConfig();
        return smoqueConfig;
    }

    private QueueHeader mockHeader(){
        queueHeader = mock(QueueHeader.class);
        doAnswer(invocation -> this.lastMessageMs)
                .when(queueHeader).getLastMessageTimeStamp();
        doAnswer(invocation -> this.lastMessagePosition)
                .when(queueHeader).getLastMessagePosition();
        return queueHeader;
    }




    @Test
    public void runPoller() throws IOException, InterruptedException {

        queuePoller.start(queueHeader);

        updateMessagesTimes();

        Thread.sleep(1000L);


        verify(smoqueTailer, times(1)).onAppend(any());


        updateMessagePosition();

        Thread.sleep(1000L);


        verify(smoqueTailer, times(2)).onAppend(any());


        queuePoller.stop();

        Thread.sleep(1000L);

        updateMessagesTimes();


        verifyNoMoreInteractions(smoqueTailer);

    }

    private void updateMessagesTimes() {
        lastMessageMs=System.currentTimeMillis();

    }

    private void updateMessagePosition(){
        lastMessagePosition=lastMessagePosition+1;
    }
}
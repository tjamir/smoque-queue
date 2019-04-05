package re.usto.smoque.queue;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by tjamir on 10/4/17.
 */
public class QueuePoller implements Runnable {

    private static Logger logger = LogManager.getLogger(QueuePoller.class);

    private SmoqueTailer tailer;
    private SmoqueQueue queue;
    private long lastWriteTimestamp;
    private long lastWriteOffset;

    private boolean active;

    public void setTailer(SmoqueTailer tailer) {
        this.tailer = tailer;
    }

    public void setQueue(SmoqueQueue queue) {
        this.queue = queue;
    }

    public void start(QueueHeader queueHeader) {
        active = true;
        updateStatus(queueHeader);
        new Thread(this, "Poller for " + tailer.toString()).start();

    }

    private void updateStatus(QueueHeader queueHeader) {
        lastWriteTimestamp = queueHeader.getLastMessageTimeStamp();
        lastWriteOffset = queueHeader.getLastMessagePosition();
    }

    public void stop() {
        active = false;
    }


    @Override
    public void run() {
        while (active) {
            try {
                QueueHeader queueHeader = queue.getQueueHeader();

                if (active && (queueHeader.getLastMessageTimeStamp() != lastWriteTimestamp  ||queueHeader.getLastMessagePosition() != lastWriteOffset)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("New message! previous write " + lastWriteTimestamp+ "/"+lastWriteOffset + " new write: "
                                + queueHeader.getLastMessageTimeStamp()+"/"+queueHeader.getLastMessagePosition());
                    }
                    tailer.onAppend(queueHeader);
                    updateStatus(queueHeader);
                }
                LockSupport.parkNanos(queue.getSmoqueConfig().getPollingIntervalNanos());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

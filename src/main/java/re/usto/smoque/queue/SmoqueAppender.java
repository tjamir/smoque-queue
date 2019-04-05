package re.usto.smoque.queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import re.usto.smoque.SmoqueConfig;
import re.usto.smoque.SmoqueWire;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by tjamir on 9/20/17.
 */
public class SmoqueAppender {

    private Logger logger = LogManager.getLogger(SmoqueAppender.class);

    private SmoqueQueue smoqueQueue;

    private SmoqueConfig smoqueConfig;

    public SmoqueQueue getSmoqueQueue() {
        return smoqueQueue;
    }


    public void append(String producerId, byte[] payload) throws IOException, TimeoutException {
        if (!smoqueQueue.getExclusiveLock(smoqueConfig.getAcquireTimeoutNanos())) {
            throw new TimeoutException("Timeout trying to retrieve exclusive lock");
        }
        try {
            byte[] producerBytes = producerId.getBytes("utf-8");
            SmoqueWire smoqueWire = new SmoqueWire();
            smoqueWire.setMagicNumber(SmoqueWire.MAGIC_NUMBER);
            smoqueWire.setProtocolVersion(SmoqueWire.PROTOCOL_VERSION);
            smoqueWire.setTotalSize(SmoqueWire.HEADER_SIZE + producerBytes.length + payload.length);
            smoqueWire.setProducerFieldSize(producerBytes.length);
            smoqueWire.setPayLoadLength(payload.length);
            smoqueWire.setExpiration(System.currentTimeMillis() + smoqueConfig.getMessageTimeoutMs());
            smoqueWire.setProducerId(producerBytes);
            smoqueWire.setPayLoad(payload);


            QueueHeader queueHeader = smoqueQueue.getQueueHeader();

            long offset = queueHeader.getNextMessagePosition();
            if ((offset + smoqueWire.getTotalSize()) > queueHeader.getFileSize()) {
                if (offset < queueHeader.getFileSize()) {
                    smoqueQueue.putRingReset(offset);
                }
                offset = QueueHeader.LENGTH;
            }

            long nextMessagePosition = offset + smoqueWire.getTotalSize();
            if (nextMessagePosition > queueHeader.getFileSize()) {
                nextMessagePosition = QueueHeader.LENGTH;
            }
            logger.debug("Writing to offset " + offset + " \nwire: " + smoqueWire.toString()
                        + " \n nextMessage" + nextMessagePosition);
            queueHeader.setLastMessagePosition(offset).setLastMessageTimeStamp(System.currentTimeMillis())
                    .setNextMessagePosition(nextMessagePosition);
            smoqueQueue.putWire(offset, smoqueWire);
                logger.debug("Putting header "+queueHeader);
            smoqueQueue.putQueueHader(queueHeader);

        } finally {
            smoqueQueue.releaseLock();
        }
    }


    public SmoqueAppender setSmoqueQueue(SmoqueQueue smoqueQueue) {
        this.smoqueQueue = smoqueQueue;
        return this;
    }

    public SmoqueAppender setSmoqueConfig(SmoqueConfig smoqueConfig) {
        this.smoqueConfig = smoqueConfig;
        return this;
    }
}

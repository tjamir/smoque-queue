package re.usto.smoque.queue;

/**
 * Created by tjamir on 9/20/17.
 */
public class QueueHeader {

    public static final int LENGTH = 5 * 8 * 8;

    public static final int LOCK_OFFSET = 8 * 1;

    private long fileSize;

    private long lockOwner;

    private long lastMessageTimeStamp;

    private long lastMessagePosition;

    private long nextMessagePosition;


    public long getFileSize() {
        return fileSize;
    }

    public QueueHeader setFileSize(long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public long getLockOwner() {
        return lockOwner;
    }

    public QueueHeader setLockOwner(long lockOwner) {
        this.lockOwner = lockOwner;
        return this;
    }

    public long getLastMessageTimeStamp() {
        return lastMessageTimeStamp;
    }

    public QueueHeader setLastMessageTimeStamp(long lastMessageTimeStamp) {
        this.lastMessageTimeStamp = lastMessageTimeStamp;
        return this;
    }

    public long getLastMessagePosition() {
        return lastMessagePosition;
    }

    public QueueHeader setLastMessagePosition(long lastMessagePosition) {
        this.lastMessagePosition = lastMessagePosition;
        return this;
    }

    public long getNextMessagePosition() {
        return nextMessagePosition;
    }

    public QueueHeader setNextMessagePosition(long nextMessagePosition) {
        this.nextMessagePosition = nextMessagePosition;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueHeader that = (QueueHeader) o;

        if (fileSize != that.fileSize) return false;
        if (lockOwner != that.lockOwner) return false;
        if (lastMessageTimeStamp != that.lastMessageTimeStamp) return false;
        if (lastMessagePosition != that.lastMessagePosition) return false;
        return nextMessagePosition == that.nextMessagePosition;
    }

    @Override
    public int hashCode() {
        int result = (int) (fileSize ^ (fileSize >>> 32));
        result = 31 * result + (int) (lockOwner ^ (lockOwner >>> 32));
        result = 31 * result + (int) (lastMessageTimeStamp ^ (lastMessageTimeStamp >>> 32));
        result = 31 * result + (int) (lastMessagePosition ^ (lastMessagePosition >>> 32));
        result = 31 * result + (int) (nextMessagePosition ^ (nextMessagePosition >>> 32));
        return result;
    }

    public boolean isEmpty(){
        return this.fileSize==0;
    }

    @Override
    public String toString() {
        return "QueueHeader{" +
                "fileSize=" + fileSize +
                ", lockOwner=" + lockOwner +
                ", lastMessageTimeStamp=" + lastMessageTimeStamp +
                ", lastMessagePosition=" + lastMessagePosition +
                ", nextMessagePosition=" + nextMessagePosition +
                '}';
    }

    public boolean isLocked() {
        return lockOwner!=0L;
    }
}

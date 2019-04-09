# Smoque-queue

## Definition

**SMOQUE** Stands for *Shared Memory Object Queue" is a shared memory lib for simple communication between differents JVM processes.  

## Motivation

This project was designed to estabilish communication between Java processes in a *Software Defined Storage* environment. We used some existent message passing libraries, but none could fullfill our needs. These are some of our requirements:

1. Low protocol overhead
1. Low latency
1. No logging or replay
1. Multiple producers and multiple consumer
1. Large messages (1MB or greater)

## Basic usage

### Instantiating Smoque

This is the most simple possible instantiation:

```java
Smoque smoque=new Smoque(new SmoqueConfig());
```

However, it is possible to make some small tweaks on Smoque:

```java
SmoqueConfig smoqueConfig=new SmoqueConfig();
//Sets the folder where are the memory mapped files
//The default value is the current folder
smoqueconfig.setPath(Paths.get("my_shm_folder")) 
//Time in nanos for polling new messages. Default is 100 ns
.setPollingIntervalNanos(200)
//Time to wait in case a memory mapped file is being used
// for writing by another proccess. Default is 20 ns
.setAcquireIntervalNanos(50)
//Maximun timeout to wait for a acquire. If zero is given,
//there is no timeout. Default value is 0
.setAcquireTimeoutNanos(200)
//Timeout for each message. Any message with exired timeout
//should be ignored by consumers
.setMessageTimeoutMs(1000);
Smoque smoque=new Smoque(smoqueConfig);
```

### Instantiating a Queue

To instantiate a Queue, one should set a name and a maximum length (in bytes).

```java
//Creating a queue with name "myQueue" with 500MB length
SmoqueQueue myQueue = smoque.getQueue("myQueue", 500*1024*1024);
```

Queues are memory mapped files composed by two parts: a header, where are some queue metadata, and a body, where the messages are placed.  

These are the headers fields:

**filesize:** Total file size, in bytes.  
**lockOwner:** The process id locking this queue for writing, zero if there is no locks.  
**lastMessageTimestamp:** The timestamp in millis when last message was written in this queue.  
**lastMessagePosistion:** The offset where the last message written was placed.
**nextMessagePosition:** The offset where the next message to be written should be placed.

### Producing to a queue

To produce to a queue, one should get a Appender for that queue. It can be acquired with the method `getAppender()` in the class `Queue`. 

```java
//Acquiring an appender for queue myQueue
SmoqueAppender smoqueAppender = myQueue.getAppender();
```
To append a message, one should  use the method `append()` from class `SmoqueAppender`. It receives a `String` that represents an identification of the producer, and a byte array, which contains the message itself.

```java
smoqueAppender.append("me", "Hello, World!");
```



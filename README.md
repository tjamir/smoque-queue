# Smoque-queue

## Definition

**SMOQUE** Stands for *Shared Memory Object Queue" is a shared memory lib for simple communication between differents JVM processes.  

## Motivation

This project was designed to estabilish communication between Java processes in a *Software Defined Storage* environment. We used some existent message passing libraries, but none could fullfill our needs. These are some of our requirements:

1. Low protocol overhead
1. Low latency
1. No logging or replay
1. Multiple producers and multiple consumer

## Basic usage

### Instantiating Smoque

This is the most simple possible instantiation:

```
Smoque smoque=new Smoque(new SmoqueConfig());
```

However, it is possible to make some small tweaks on Smoque:

```
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

## 

# Test case to show flink cannot handle Enums in keyBy()

I've reproduced this on a 4 taskmanager (8 slots per taskmanager) cluster. Running the job with a parallelism of 32.

When I use an enum as Key I receive the following exception:

~~~~
java.lang.IllegalArgumentException
	at org.apache.flink.util.Preconditions.checkArgument(Preconditions.java:123)
	at org.apache.flink.runtime.state.heap.HeapPriorityQueueSet.globalKeyGroupToLocalIndex(HeapPriorityQueueSet.java:158)
	at org.apache.flink.runtime.state.heap.HeapPriorityQueueSet.getDedupMapForKeyGroup(HeapPriorityQueueSet.java:147)
	at org.apache.flink.runtime.state.heap.HeapPriorityQueueSet.getDedupMapForElement(HeapPriorityQueueSet.java:154)
	at org.apache.flink.runtime.state.heap.HeapPriorityQueueSet.add(HeapPriorityQueueSet.java:121)
	at org.apache.flink.runtime.state.heap.HeapPriorityQueueSet.add(HeapPriorityQueueSet.java:49)
	at org.apache.flink.streaming.api.operators.InternalTimerServiceImpl.registerProcessingTimeTimer(InternalTimerServiceImpl.java:201)
	at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator$Context.registerProcessingTimeTimer(WindowOperator.java:876)
	at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.registerCleanupTimer(WindowOperator.java:605)
	at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.processElement(WindowOperator.java:409)
	at org.apache.flink.streaming.runtime.io.StreamInputProcessor.processInput(StreamInputProcessor.java:202)
	at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask.run(OneInputStreamTask.java:105)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:300)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:711)
	at java.lang.Thread.run(Thread.java:748)
~~~~


How ever when this same enum is inside of an avro class everything works fine.

See the 2 keyselectors:

TestAvroEnum.java line 29 vs line 33.

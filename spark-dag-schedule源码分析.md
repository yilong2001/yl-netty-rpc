# DAGScheduler

## 1 创建过程
（1）DAGScheduler在SparkContext的创建过程中create。从这一点也可以看出，DAGScheduler运行在driver节点上；

（2）节选代码
```
// Create and start the scheduler val 
(sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
 _schedulerBackend = sched 
_taskScheduler = ts 
_dagScheduler = new DAGScheduler(this) 
_heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)  

// start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's 
// constructor
 _taskScheduler.start()
```

在create DAGScheduler构造函数中，初始化TaskScheduler。然后，由TaskScheduler负责start整个调度过程。

（3）TaskScheduler的start 代码
```
override def start() {   
    backend.start()    
    if (!isLocal and conf.getBoolean("spark.speculation", false)) {     
        logInfo("Starting speculative execution thread")     
        speculationScheduler.scheduleWithFixedDelay(new Runnable {       
            override def run(): Unit = Utils.tryOrStopSparkContext(sc) {         
                checkSpeculatableTasks()       
            }     
        }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)   
    }
 }
```

## 2 LiveListenerBus的Liseners
在初始化DAGScheduler的时候，要用到LiveListenerBus。LiveListenerBus在task/stage过程中，可以把中间状态发布给相关的Listeners。

在SparkContext注册的LiveListenerBus的Listeners：

### 2.1 JobProgressListener
```
_jobProgressListener = new JobProgressListener(_conf) 
listenerBus.addListener(jobProgressListener)
```

### 2.2 SparkUI
```
SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,   _env.securityManager, appName, startTime = startTime)
```

以及在SparkUI：
```
val _jobProgressListener: JobProgressListener = jobProgressListener.getOrElse {   
    val listener = new JobProgressListener(conf)   
    listenerBus.addListener(listener)   
    listener 
}

  val environmentListener = new EnvironmentListener val 
storageStatusListener = new StorageStatusListener(conf) 
val executorsListener = new ExecutorsListener(storageStatusListener, conf)
 val storageListener = new StorageListener(storageStatusListener) 
val operationGraphListener = new RDDOperationGraphListener(conf)  

listenerBus.addListener(environmentListener)
 listenerBus.addListener(storageStatusListener) 
listenerBus.addListener(executorsListener) 
listenerBus.addListener(storageListener) 
listenerBus.addListener(operationGraphListener)
```

### 2.3 Logging
```
val logger =   new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,     _conf, _hadoopConfiguration) 
logger.start() 
listenerBus.addListener(logger)
```

### 2.4 通过spark.extraListeners扩展Listeners
```
val listenerClassNames: Seq[String] =   conf.get("spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
```

## 3 LiveListenerBus处理的messages
### 3.1 DAGScheduler
处理的消息类型，包括 JobStart、JobEnd、TaskStart、TaskEnd、TaskGetingResult、StageSubmitted等。

从这些消息类型可以看到，DAGScheduler是用户任务调度的核心，处理内容包含了Job、Task、Stage。
```
listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))

listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))

listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))

listenerBus.post(SparkListenerTaskGettingResult(taskInfo))

listenerBus.post(   SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

listenerBus.post(   SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

listenerBus.post(SparkListenerTaskEnd(    stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

listenerBus.post(   SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))

listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))

listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))

listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
```

### 3.2 SparkContext
SparkContext处理ApplicationStart、ApplicationEnd、EnvimentUpdate等几种类型的消息。

从这些内容可以看到，SparkContext管理对象是Application。

(1) UnpersistRDD(在SparkContext中)
```
listenerBus.post(SparkListenerUnpersistRDD(rddId))
```

(2) Application Start(在SparkContext中)
```
listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),   startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls))
```

(3) Application End(在SparkContext中)
```
listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
```

(4) EnvimentUpdate(在SparkContext中)
```
val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails) listenerBus.post(environmentUpdate)
```

### 3.3 BlockManagerMasterEndpoint
BlockManagerMasterEndpoint处理Block相关的消息，Add Block、Remove Block、Update Block。

Spark中，BlockManager运行在每个节点（driver+worker），提供获取本地、remote block的接口。

BlockManager在功能上类似hdfs，Block与hdfs的block相似（但没有副本，如果出错重新计算）。

Task运行过程中需要的block，会从local和remote获取。Block的分配以partition为基础。

BlockManagerMasterEndpoint运行在Driver端，BlockManagerSlaveEndpoint运行在Executor。

运行在Executor的blockManager在初始化的时候会注册自身到BlockManagerMaster，从而获取到 Driver端的BlockManagerMasterEndpoint，因而与Driver直接进行RPC通信。
```
listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))

listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))

listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,     Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
```

### 3.4 SQLExecution
```
sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(   executionId, callSite.shortForm, callSite.longForm, queryExecution.toString,   SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan), System.currentTimeMillis()))

sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(executionId, System.currentTimeMillis()))
```

### 3.5 JobScheduler
JobScheduler处理的消息类型：BatchSubmitted、BatchStarted、BatchCompleted、OutputOperationStarted、OutputOperationCompleted等。

从消息类型可以看到，主要处理内容是与Batch、OutputOperation相关。

在Spark的概念上，只有Action算子和Save操作会作为程序的Job单元被调度。而Transformation算子（一系列Task）则由Job单元触发、被动执行。

```
listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))

listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))

listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))

listenerBus.post(StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))

listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
```

### 3.6 CoarseGrainedSchedulerBackend
```
listenerBus.post(   SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))

listenerBus.post(   SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
```

### 3.7 ReceiverTracker
```
listenerBus.post(StreamingListenerReceiverStarted(receiverTrackingInfo.toReceiverInfo))

listenerBus.post(StreamingListenerReceiverStopped(newReceiverTrackingInfo.toReceiverInfo))

listenerBus.post(StreamingListenerReceiverError(newReceiverTrackingInfo.toReceiverInfo))
```

### 3.8 SQLMetrics
```
sc.listenerBus.post(   SparkListenerDriverAccumUpdates(executionId.toLong, metrics.map(m => m.id -> m.value)))
```

### 3.9 LocalSchedulerBackendEndpoint
```
listenerBus.post(SparkListenerExecutorAdded(   System.currentTimeMillis,   executorEndpoint.localExecutorId,   new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
```

### 3.10 BlacklistTracker
```
listenerBus.post(SparkListenerExecutorUnblacklisted(now, exec))

listenerBus.post(SparkListenerNodeUnblacklisted(now, node))

listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, newTotal))

listenerBus.post(SparkListenerNodeBlacklisted(now, node, blacklistedExecsOnNode.size))
```

### 3.11 SharedState
```
externalCatalog.addListener(new ExternalCatalogEventListener {   override def onEvent(event: ExternalCatalogEvent): Unit = {     sparkContext.listenerBus.post(event)   } })
```

### 3.12 MesosFineGrainedSchedulerBackend
```
listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), slaveId,   // TODO: Add support for log urls for Mesos   new ExecutorInfo(o.host, o.cores, Map.empty)))

listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), slaveId, reason))
```

## 4 事件循环（DAGSchedulerEventProcessLoop）
### 4.1 机制
独立线程，使用LinkedBlockingDeque作为消息队列。
```
DAGSchedulerEventProcessLoop   
EventLoop 
```

## 5 JobSubmitted事件
JobSubmitted消息组成：Jobid、finalRdd、function2、Partitions、CallSite(用户代码位置)、Listener(Waiter:等待Stage完成)、Properties。

JobSubmmited只发生在runJob  submitJob   post(JobSubmitted(…)) 流程。

### 5.1 JobSubmitted发生场景：SparkContext
Run an action job on the given RDD and pass all the results to the resultHandler function as they arrive。
```
def runJob[T, U](     rdd: RDD[T],     func: (TaskContext, Iterator[T]) => U,     partitions: Seq[Int],     callSite: CallSite,     resultHandler: (Int, U) => Unit,     properties: Properties)
```

runJob的主要调用发生在SparkContext中。

Run a function on a given set of partitions in an RDD and pass the results to the given handler function. 

This is the main entry point for all actions in Spark.
```
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
  }
```


### 5.2 runJob发生场景：LocalRDDCheckpointData
在doCheckPoint的时候，如果 某个partition 有丢失的 Block，会触发 runJob。
```
if (missingPartitionIndices.nonEmpty) {   
    rdd.sparkContext.runJob(rdd, action, missingPartitionIndices) 
}
```

### 5.3 runJob发生场景：ReliableCheckpointRDD
在CheckPoint 文件Write RDD时，write文件动作会触发runJob。
```
  def writeRDDToCheckpointDirectory[T: ClassTag](
      originalRDD: RDD[T],
      checkpointDir: String,
      blockSize: Int = -1): ReliableCheckpointRDD[T] = {

    ......
    
    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)

    ......
    newRDD
  }
```

### 5.4 runJob发生场景：SparkHadoopMapReduceWriter
write动作会触发runJob，executeTask、commitJob。
```
  def write[K, V: ClassTag](
      rdd: RDD[(K, V)],
      hadoopConf: Configuration): Unit = {
    ......
    // Try to write all RDD partitions as a Hadoop OutputFormat.
    try {
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        executeTask(
          context = context,
          jobTrackerId = jobTrackerId,
          sparkStageId = context.stageId,
          sparkPartitionId = context.partitionId,
          sparkAttemptNumber = context.attemptNumber,
          committer = committer,
          hadoopConf = conf.value,
          outputFormat = format.asInstanceOf[Class[OutputFormat[K, V]]],
          iterator = iter)
      })
    .....
```

### 5.5 runJob发生场景：RDD
在foreach、foreachPartition、collect、toLocalIterator、reduce、fold、aggregate、count、take等会触发runJob。

因此，这些动作，都属于Action算子。

### 5.6 runJob发生场景：KafkaRDD
在take record的时候，会触发runJob，job返回的结果为ConsumerRecord类型的数组。
```
override def take(num: Int): Array[ConsumerRecord[K, V]] = {
……
val res = context.runJob(   this, (tc: TaskContext, it: Iterator[ConsumerRecord[K, V]]) =>   it.take(parts(tc.partitionId)).toArray, parts.keys.toArray )
```

### 5.7 事件处理
处理流程：

创建 ResultStage  SubmitStage  寻找Stage父依赖RDD (窄依赖／宽依赖)  如果父依赖不可用，首先执行父依赖Stage  直到所有父依赖 RDD可用

从这个过程可以看出，Spark的Job处理是一种Lazy模式，只有在需要(Action算子／Save文件)的时候，才会触发计算过程。
```
case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>   dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

private[scheduler] def handleJobSubmitted(jobId: Int,     finalRDD: RDD[_],     func: (TaskContext, Iterator[_]) => _,     partitions: Array[Int],     callSite: CallSite,     listener: JobListener,     properties: Properties) {   
var finalStage: ResultStage = null
……
finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite) ……
  finalStage.setActiveJob(job)
……   submitStage(finalStage)
}
```

## 6 MapStageSubmitted事件
消息由JobId、ShuffleDependency、CallSite、JobListener(Waiter:等待Stage完成)、Properties组成。
主要作用是生成宽依赖即create ShuffleMapStage并SubmitStage。

### 6.1 submitMapStage发生场景：SparkContext
submitMapStage的唯一入口是SparkContext。

Submit a map stage for execution. This is currently an internal API only, but might be promoted to DeveloperApi in the future.
```
private[spark] def submitMapStage[K, V, C](dependency: ShuffleDependency[K, V, C])     : SimpleFutureAction[MapOutputStatistics] = {   assertNotStopped()   val callSite = getCallSite()   var result: MapOutputStatistics = null   val waiter = dagScheduler.submitMapStage(     dependency,     (r: MapOutputStatistics) => { result = r },     callSite,     localProperties.get)   new SimpleFutureAction[MapOutputStatistics](waiter, result) }
```

### 6.2 submitMapStage发生场景：ExchangeCoordinator
在doEstimationIfNecessary方法里调用。

if (shuffleDependency.rdd.partitions.length != 0) {   // submitMapStage does not accept RDD with 0 partition.   // So, we will not submit this dependency.   submittedStageFutures +=     exchange.sqlContext.sparkContext.submitMapStage(shuffleDependency) }


### 6.3 submitMapStage发生场景：在runJob
如上所述，在runJob调用时，会触发 Parent Stage的计算，如果Parent Stage是Shuffle过程，则会触发ShuffleMapStage的创建，并在submitStage中执行。

## 7 ExecutorAdded事件
消息由execId 和 host组成。作用是通知executor分配完成。

### 7.1 executorAdded发生场景：submitApplication阶段
调用过程：

```
（1）在ApplicationMaster的submitApplication阶段，触发YarnAllocator的allocateResources；

（2）在allocateResources触发new ExecutorRunnable；

（3）在ExecutorRunnable会构造命令参数：

Seq("org.apache.spark.executor.CoarseGrainedExecutorBackend",   "--driver-url", masterAddress,   "--executor-id", executorId,   "--hostname", hostname,   "--cores", executorCores.toString,   "--app-id", appId)

（4）ExecutorRunnable在构造命令之后，会触发执行命令，也就是启动Ececutor，即启动运行CoarseGrainedExecutorBackend

（5）CoarseGrainedExecutorBackend启动之后，会给Driver发送RegisterExecutor消息

rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>   // This is a very fast action so we can use "ThreadUtils.sameThread"   driver = Some(ref)   ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls)) }

（6）CoarseGrainedSchedulerBackend会处理RegisterExecutor消息（运行在Driver端，参考SparkContext启动过程）

override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {    case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>

（7）触发CoarseGrainedSchedulerBackend的makeOffers方法
（8）触发TaskSchedulerImpl的resourceOffers方法
val taskDescs = CoarseGrainedSchedulerBackend.this.synchronized {   
    // Filter out executors under killing   
    val activeExecutors = executorDataMap.filterKeys(executorIsAlive)   
    val workOffers = activeExecutors.map { case (id, executorData) =>     new WorkerOffer(id, executorData.executorHost, executorData.freeCores)   }.toIndexedSeq   
    scheduler.resourceOffers(workOffers)
 }

（9）触发TaskSchedulerImpl的executorAdded，调用DAGScheduler.executorAdded函数
def executorAdded(execId: String, host: String) {   dagScheduler.executorAdded(execId, host) }

（10）DAGScheduler发送ExecutorAdded消息，并处理此消息
def executorAdded(execId: String, host: String): Unit = {   eventProcessLoop.post(ExecutorAdded(execId, host)) }

private[scheduler] def handleExecutorAdded(execId: String, host: String)
```

### 7.2 executorAdded发生场景：BlockManager的initialize阶段
调用过程：

（1）创建BlockManager以及shuffleServer（也是BlockManager）
```
val id =   BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)  val idFromMaster = master.registerBlockManager( id, maxOnHeapMemory,maxOffHeapMemory,slaveEndpoint)

shuffleServerId = if (externalShuffleServiceEnabled) {   
    logInfo(s"external shuffle service port = $externalShuffleServicePort")  
    BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort) 
} else {  
    blockManagerId   
}
```

（2）调用ExternalShuffleClient. registerWithShuffleServer，给shuffleServer client节点发送RegisterExecutor消息
```
try (TransportClient client = clientFactory.createUnmanagedClient(host, port)) {   ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();   client.sendRpcSync(registerMessage, 5000 /* timeoutMs */); }
```

## 8 BeginEvent事件
BeginEvent来源于CoarseGrainedSchedulerBackend的makeOffers，也就是在分配Executor的时候发生。

### 8.1 BeginEvent发生场景：TaskFinish
```
override def receive: PartialFunction[Any, Unit] = {   case StatusUpdate(executorId, taskId, state, data) =>     scheduler.statusUpdate(taskId, state, data.value)     if (TaskState.isFinished(state)) { makeOffers(executorId)
```

### 8.2 BeginEvent发生场景：RegisterExecutor
```
override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {   case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls) =>
```

发生上下文：

1）Executor的Start阶段（CoarseGrainedExecutorBackend）
```
override def onStart() {   logInfo("Connecting to driver: " + driverUrl)   rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>     // This is a very fast action so we can use "ThreadUtils.sameThread"     driver = Some(ref)     ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls))
```

2）在Shuffle阶段（ExternalShuffleClient）
```
public void registerWithShuffleServer(     String host,     int port,     String execId,     ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {   checkInit();   try (TransportClient client = clientFactory.createUnmanagedClient(host, port)) {     ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
```

### 8.3 BeginEvent发生场景：ReviveOffers
```
override def receive: PartialFunction[Any, Unit] = {
case ReviveOffers =>   makeOffers()
```

发生上下文：DriverEndpoint的start时（即有新的Spark任务，就会触发Executor的分配）
```
class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])   extends ThreadSafeRpcEndpoint with Logging { 
    override def onStart() {     
        reviveThread.scheduleAtFixedRate(new Runnable {       
        override def run(): Unit = Utils.tryLogNonFatalError {         
        Option(self).foreach(_.send(ReviveOffers))       
        }     
    }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)   
}
```



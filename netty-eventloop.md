## netty eventloop
- [1. epoll](#1-epoll)
- [2. Selector](#2-Selector)
- [3. ServerBootStrap](#3-BootStrap)

## epoll
(1) epoll_create 会触发内核创建一个 eventpoll 结构体(可以理解为一个文件系统)
```
struct eventpoll{
    ....
    /*红黑树的根节点，这颗树中存储着所有添加到epoll中的需要监控的事件*/
    struct rb_root  rbr;
    /*双链表中则存放着将要通过epoll_wait返回给用户的满足条件的事件*/
    struct list_head rdlist;
    ....
};
```

(2) 每个epoll对象都有一个独立的 eventpoll 对象

(3) eventpoll 保存通过 epoll_ctl 方法向 epoll 对象中添加进来的事件
```
每个 epoll 事件对应的结构体
struct epitem{
    struct rb_node  rbn;//红黑树节点
    struct list_head    rdllink;//双向链表节点
    struct epoll_filefd  ffd;  //事件句柄信息
    struct eventpoll *ep;    //指向其所属的eventpoll对象
    struct epoll_event event; //期待发生的事件类型
}
```

(4) 这些事件都会挂载在红黑树中

(5) 重复添加的事件可以通过红黑树而高效识别

(6) 所有添加到 epoll 中的事件都会与设备(网卡)驱动程序建立回调

(7) 也即，相应事件发生时会调用这个回调。这个回调在内核中叫ep_poll_callback,它会将发生的事件添加到 rdlist 双向链表中

### 主要接口
(1) epoll_create(): 此调用返回一个句柄，之后所有的使用都依靠这个句柄来标识

(2) epoll_ctl() : 此调用向 eventpoll 添加、删除、修改感兴趣的事件，0标识成功，-1表示失败

(3) epoll_wait() : 此调用收集在 epoll 中已经发生的事件，检查 eventpoll 中 rdlist 双链表中是否有 epitem 元素即可

### java nio epoll 100% CPU bug
```
bug发生时：不管有没有已选择的SelectionKey，Selector.select()方法总是不会阻塞并且会立刻返回。
这违反了 Javadoc 对 Selector.select() 方法的描述。
Javadoc 的描述：Selector.select() must not unblock if nothing is selected(Selector.select()方法若未选中任何事件将会阻塞) 。
```

```
发生bug的代码如下：

while (true) { 
int selected = selector.select();//should blocking, but none 
Set<SelectedKeys> readyKeys = selector.selectedKeys(); 
Iterator iterator = readyKeys.iterator(); 
while (iterator.hasNext()) {
```

## Selector
(1) Selector是java NIO中实现I/O多路复用的关键类。

(2) Selector通过一个线程管理多个Channel(socket conn)，从而管理多个网络连接

(3) 需要将 Channel 注册到 Selector 中以实现 Selector 对 Channel 的管理

(4) 一个Channel可以注册到多个不同的Selector

### SelectionKey
(1) Channel注册到Selector后会返回一个SelectionKey对象

(2) SelectionKey 代表 Channel 和它注册的 Selector 间的关系

(3) SelectionKey 有两个很重要的属性：interestOps、readyOps

(4) interestOps : 希望Selector监听Channel的哪些事件

(5) readyOps : 当Channel有感兴趣的事件发生，就会将感兴趣的事件设置到 readyOps 中

### keys : SelectionKey
所有注册到Selector的Channel所表示的SelectionKey都会存在于该集合中。keys元素的添加会在Channel注册到Selector时发生

### selectedKeys : SelectionKey
对应的Channel在上一次操作 selection 期间被检测到至少有一种 SelectionKey 感兴趣的操作已经就绪

### cancelledKeys : SelectionKey
执行了取消操作的SelectionKey会被放入到该集合中。该集合是keys的一个子集

## server
### BossGroup
* 通常是一个 EventExecutor 执行线程
* 在 server 端，用于管理 bind socket address 的 channel(相当于管理 bind 类型的文件)
* 在 client 端，用于管理 connect remote peer 的 channel(相当于管理 connect 类型的文件)

### WorkerGroup
* 多个 EventExecutor 执行线程
* 在 server 端，管理所有从 client 端来的 channel
```
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            final Channel child = (Channel) msg;

            child.pipeline().addLast(childHandler);

            setChannelOptions(child, childOptions, logger);

            for (Entry<AttributeKey<?>, Object> e: childAttrs) {
                child.attr((AttributeKey<Object>) e.getKey()).set(e.getValue());
            }

            try {
                childGroup.register(child).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            forceClose(child, future.cause());
                        }
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }
```


### NioEventLoopGroup 
* EventExecutor[]：一组 NioEventLoop 组成的 EventLoop 池(即线程池), 每个 EventExecutor 管理一个 Selector(即管理多个 Socket 连接: SelectionKey )
* EventExecutorChooser : 选择 next 的策略对象

### NioEventLoop
#### run (读 Selector / 处理 Task)
(1) Calculate SelectStrategy : Continue/Select/Other

(2) Process SelectedKeys
```
NioChannel.finishConnect(Channel.SelectionKey.OP_CONNECT)
NioChannel.unsafe().forceFlush(SelectionKey.OP_WRITE)
NioChannel.read(SelectionKey.OP_READ / SelectionKey.OP_ACCEPT)
```

(3) Run All Task (SingleThreadEventExecutor)
```
execute(task)
```


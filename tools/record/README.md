event的处理流程：
- 创建 EventRecorder 对象，通过其提供的 Event 等方法，创建好event对象
- 将创建出来的对象发送给 EventBroadcaster 中的channel中
- EventBroadcaster 通过后台运行的goroutine，从管道中取出事件，并广播给提前注册好的handler处理
- 当输出log的handler收到事件就直接打印事件
- 当 EventSink handler收到处理事件就通过预处理之后将事件发送给apiserver
- 其中预处理包含三个动作，1、限流 2、聚合 3、计数
- apiserver收到事件处理之后就存储在etcd中

event不保证100%事件写入，为了后端服务etcd的可用性，event事件在整个集群中产生是非常频繁，尤其在服务不稳定的时候，而相比Deployment,Pod等其他资源，没那么的重要。
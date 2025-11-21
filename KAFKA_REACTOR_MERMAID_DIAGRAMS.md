# Kafka Reactor - Mermaid 架构图

## 1. SocketServer 启动流程（简化版）

```mermaid
graph TB
    Start[SocketServer.start] --> RC[RequestChannel.new]
    Start --> CQ[ConnectionQuotas.new]
    Start --> MP[SimpleMemoryPool.new]
    Start --> P0[Processor.new × 3]
    P0 --> PT[Thread.start × 3]
    Start --> A[Acceptor.new]
    A --> AI[Acceptor.init]
    AI --> AT[Thread.start]
    Start --> H[RequestHandler.new × 8]
    H --> HT[Thread.start × 8]

    style Start fill:#f9f,stroke:#333,stroke-width:4px
    style RC fill:#bbf,stroke:#333,stroke-width:2px
    style CQ fill:#bbf,stroke:#333,stroke-width:2px
    style MP fill:#bbf,stroke:#333,stroke-width:2px
    style P0 fill:#bfb,stroke:#333,stroke-width:2px
    style A fill:#fbf,stroke:#333,stroke-width:2px
    style H fill:#ffb,stroke:#333,stroke-width:2px
```

## 2. SocketServer 启动流程（详细版 - 按执行顺序）

```mermaid
graph TB
    Start[SocketServer.start] --> Step1[1. 创建共享组件]

    Step1 --> RC[RequestChannel.new<br/>capacity: 500]
    Step1 --> CQ[ConnectionQuotas.new<br/>maxPerIp, maxTotal]
    Step1 --> MP[SimpleMemoryPool.new<br/>size: 100MB]

    RC --> Step2[2. 创建Processors]
    CQ --> Step2
    MP --> Step2

    Step2 --> P0[Processor 0]
    Step2 --> P1[Processor 1]
    Step2 --> P2[Processor 2]

    P0 --> PT0[Thread.start<br/>processor-0]
    P1 --> PT1[Thread.start<br/>processor-1]
    P2 --> PT2[Thread.start<br/>processor-2]

    PT0 --> Step3[3. 创建Acceptor]
    PT1 --> Step3
    PT2 --> Step3

    Step3 --> A[Acceptor.new<br/>processors: List]
    A --> AI[Acceptor.init<br/>ServerSocketChannel]
    AI --> AT[Thread.start<br/>acceptor-PLAINTEXT]

    AT --> Step4[4. 创建Handlers]

    Step4 --> H0[Handler 0]
    Step4 --> H1[Handler 1]
    Step4 --> H2[Handler 2]
    Step4 --> H3[Handler 3]
    Step4 --> H4[Handler 4]
    Step4 --> H5[Handler 5]
    Step4 --> H6[Handler 6]
    Step4 --> H7[Handler 7]

    H0 --> HT0[Thread.start<br/>handler-0]
    H1 --> HT1[Thread.start<br/>handler-1]
    H2 --> HT2[Thread.start<br/>handler-2]
    H3 --> HT3[Thread.start<br/>handler-3]
    H4 --> HT4[Thread.start<br/>handler-4]
    H5 --> HT5[Thread.start<br/>handler-5]
    H6 --> HT6[Thread.start<br/>handler-6]
    H7 --> HT7[Thread.start<br/>handler-7]

    HT0 --> Done[启动完成]
    HT1 --> Done
    HT2 --> Done
    HT3 --> Done
    HT4 --> Done
    HT5 --> Done
    HT6 --> Done
    HT7 --> Done

    style Start fill:#ff9999,stroke:#333,stroke-width:4px
    style Step1 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style Step2 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style Step3 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style Step4 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style RC fill:#99ccff,stroke:#333,stroke-width:2px
    style CQ fill:#99ccff,stroke:#333,stroke-width:2px
    style MP fill:#99ccff,stroke:#333,stroke-width:2px
    style P0 fill:#99ff99,stroke:#333,stroke-width:2px
    style P1 fill:#99ff99,stroke:#333,stroke-width:2px
    style P2 fill:#99ff99,stroke:#333,stroke-width:2px
    style A fill:#ff99ff,stroke:#333,stroke-width:2px
    style H0 fill:#ffff99,stroke:#333,stroke-width:2px
    style H1 fill:#ffff99,stroke:#333,stroke-width:2px
    style H2 fill:#ffff99,stroke:#333,stroke-width:2px
    style H3 fill:#ffff99,stroke:#333,stroke-width:2px
    style H4 fill:#ffff99,stroke:#333,stroke-width:2px
    style H5 fill:#ffff99,stroke:#333,stroke-width:2px
    style H6 fill:#ffff99,stroke:#333,stroke-width:2px
    style H7 fill:#ffff99,stroke:#333,stroke-width:2px
    style Done fill:#99ff99,stroke:#333,stroke-width:4px
```

## 3. 组件依赖关系图

```mermaid
graph LR
    SS[SocketServer] --> RC[RequestChannel]
    SS --> CQ[ConnectionQuotas]
    SS --> MP[MemoryPool]

    SS --> Acceptor
    SS --> P0[Processor 0]
    SS --> P1[Processor 1]
    SS --> P2[Processor 2]
    SS --> H0[Handler 0-7]

    Acceptor --> P0
    Acceptor --> P1
    Acceptor --> P2

    P0 --> RC
    P1 --> RC
    P2 --> RC
    P0 --> CQ
    P1 --> CQ
    P2 --> CQ
    P0 --> MP
    P1 --> MP
    P2 --> MP

    H0 --> RC

    style SS fill:#ff9999,stroke:#333,stroke-width:4px
    style RC fill:#99ccff,stroke:#333,stroke-width:2px
    style CQ fill:#99ccff,stroke:#333,stroke-width:2px
    style MP fill:#99ccff,stroke:#333,stroke-width:2px
    style Acceptor fill:#ff99ff,stroke:#333,stroke-width:2px
    style P0 fill:#99ff99,stroke:#333,stroke-width:2px
    style P1 fill:#99ff99,stroke:#333,stroke-width:2px
    style P2 fill:#99ff99,stroke:#333,stroke-width:2px
    style H0 fill:#ffff99,stroke:#333,stroke-width:2px
```

## 4. 线程启动时序图

```mermaid
sequenceDiagram
    participant Main as Main Thread
    participant RC as RequestChannel
    participant P0 as Processor-0
    participant P1 as Processor-1
    participant P2 as Processor-2
    participant Acc as Acceptor
    participant H as Handlers

    Main->>RC: new RequestChannel(500)
    Main->>Main: new ConnectionQuotas()
    Main->>Main: new SimpleMemoryPool()

    Main->>P0: new Processor(0)
    Main->>P0: thread.start()
    activate P0
    P0->>P0: Event Loop Running

    Main->>P1: new Processor(1)
    Main->>P1: thread.start()
    activate P1
    P1->>P1: Event Loop Running

    Main->>P2: new Processor(2)
    Main->>P2: thread.start()
    activate P2
    P2->>P2: Event Loop Running

    Main->>Acc: new Acceptor(processors)
    Main->>Acc: init()
    Main->>Acc: thread.start()
    activate Acc
    Acc->>Acc: Accept Loop Running

    Main->>H: new Handler(0-7)
    Main->>H: thread.start() × 8
    activate H
    H->>H: Process Loop Running

    Note over P0,H: 所有组件并行运行
```

## 5. 完整架构图（运行时）

```mermaid
graph TB
    Client[客户端]

    subgraph SocketServer
        subgraph SharedComponents[共享组件]
            RC[RequestChannel<br/>requestQueue: 500<br/>responseQueues: Map]
            CQ[ConnectionQuotas<br/>IP/Listener/Broker级别]
            MP[MemoryPool<br/>100MB CAS管理]
        end

        subgraph MainReactor[Main Reactor]
            Acceptor[Acceptor<br/>1个线程<br/>ServerSocketChannel]
        end

        subgraph SubReactors[Sub Reactors]
            P0[Processor 0<br/>1个线程<br/>1个Selector]
            P1[Processor 1<br/>1个线程<br/>1个Selector]
            P2[Processor 2<br/>1个线程<br/>1个Selector]
        end

        subgraph Workers[业务处理]
            H0[Handler 0-7<br/>8个线程<br/>Echo处理]
        end
    end

    Client -->|TCP连接| Acceptor
    Acceptor -->|Round-Robin| P0
    Acceptor -->|Round-Robin| P1
    Acceptor -->|Round-Robin| P2

    P0 -->|Request| RC
    P1 -->|Request| RC
    P2 -->|Request| RC

    RC -->|Request| H0
    H0 -->|Response| RC

    RC -->|Response| P0
    RC -->|Response| P1
    RC -->|Response| P2

    P0 -->|使用| CQ
    P1 -->|使用| CQ
    P2 -->|使用| CQ

    P0 -->|使用| MP
    P1 -->|使用| MP
    P2 -->|使用| MP

    P0 -->|响应| Client
    P1 -->|响应| Client
    P2 -->|响应| Client

    style Client fill:#e1f5ff,stroke:#333,stroke-width:2px
    style SharedComponents fill:#fff4e1,stroke:#333,stroke-width:2px
    style MainReactor fill:#ffe1f5,stroke:#333,stroke-width:2px
    style SubReactors fill:#e1ffe1,stroke:#333,stroke-width:2px
    style Workers fill:#fffbe1,stroke:#333,stroke-width:2px
    style RC fill:#99ccff,stroke:#333,stroke-width:2px
    style CQ fill:#99ccff,stroke:#333,stroke-width:2px
    style MP fill:#99ccff,stroke:#333,stroke-width:2px
    style Acceptor fill:#ff99ff,stroke:#333,stroke-width:2px
    style P0 fill:#99ff99,stroke:#333,stroke-width:2px
    style P1 fill:#99ff99,stroke:#333,stroke-width:2px
    style P2 fill:#99ff99,stroke:#333,stroke-width:2px
    style H0 fill:#ffff99,stroke:#333,stroke-width:2px
```

## 6. 启动流程状态图

```mermaid
stateDiagram-v2
    [*] --> Initializing: SocketServer.start()

    Initializing --> CreatingShared: 创建共享组件
    CreatingShared --> SharedReady: RequestChannel<br/>ConnectionQuotas<br/>MemoryPool

    SharedReady --> CreatingProcessors: 创建Processors
    CreatingProcessors --> ProcessorsRunning: Processor 0<br/>Processor 1<br/>Processor 2<br/>3个线程启动

    ProcessorsRunning --> CreatingAcceptor: 创建Acceptor
    CreatingAcceptor --> AcceptorInit: Acceptor.init()<br/>ServerSocketChannel
    AcceptorInit --> AcceptorRunning: 1个线程启动

    AcceptorRunning --> CreatingHandlers: 创建Handlers
    CreatingHandlers --> HandlersRunning: Handler 0-7<br/>8个线程启动

    HandlersRunning --> Running: 所有组件运行中
    Running --> [*]: shutdown()
```

## 7. 类实例化顺序（代码级别）

```mermaid
flowchart TD
    Start([SocketServer.start])

    Start --> C1{创建共享组件}
    C1 --> RC[RequestChannel requestChannel<br/>= new RequestChannel<br/>DEFAULT_REQUEST_QUEUE_SIZE]
    C1 --> CQ[ConnectionQuotas connectionQuotas<br/>= new ConnectionQuotas<br/>maxPerIp, maxTotal]
    C1 --> MP[MemoryPool memoryPool<br/>= new SimpleMemoryPool<br/>100MB, strict=true]

    RC --> C2{创建Processors}
    CQ --> C2
    MP --> C2

    C2 --> Loop1[for i = 0 to numProcessors-1]
    Loop1 --> NewP[Processor processor = new Processor<br/>i, listenerName, requestChannel,<br/>connectionQuotas, memoryPool, maxReceiveSize]
    NewP --> AddP[processors.add processor]
    AddP --> StartP[Thread thread = new Thread processor<br/>thread.setDaemon false<br/>thread.start]
    StartP --> CheckLoop1{i < numProcessors?}
    CheckLoop1 -->|Yes| Loop1
    CheckLoop1 -->|No| C3{创建Acceptor}

    C3 --> NewA[Acceptor acceptor = new Acceptor<br/>listenerName, address, processors]
    NewA --> InitA[acceptor.init]
    InitA --> StartA[Thread acceptorThread = new Thread acceptor<br/>acceptorThread.setDaemon false<br/>acceptorThread.start]

    StartA --> C4{创建Handlers}
    C4 --> Loop2[for i = 0 to numHandlers-1]
    Loop2 --> NewH[RequestHandler handler<br/>= new RequestHandler<br/>i, requestChannel]
    NewH --> AddH[handlers.add handler]
    AddH --> StartH[Thread thread = new Thread handler<br/>thread.setDaemon false<br/>thread.start]
    StartH --> CheckLoop2{i < numHandlers?}
    CheckLoop2 -->|Yes| Loop2
    CheckLoop2 -->|No| End([启动完成])

    style Start fill:#ff9999,stroke:#333,stroke-width:3px
    style End fill:#99ff99,stroke:#333,stroke-width:3px
    style C1 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style C2 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style C3 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style C4 fill:#e6f3ff,stroke:#333,stroke-width:2px
    style RC fill:#99ccff,stroke:#333,stroke-width:2px
    style CQ fill:#99ccff,stroke:#333,stroke-width:2px
    style MP fill:#99ccff,stroke:#333,stroke-width:2px
    style NewP fill:#99ff99,stroke:#333,stroke-width:2px
    style NewA fill:#ff99ff,stroke:#333,stroke-width:2px
    style NewH fill:#ffff99,stroke:#333,stroke-width:2px
```

## 使用说明

### 在Markdown中渲染

将以上任意一个代码块复制到支持mermaid的markdown编辑器中（如Typora、GitHub、GitLab等）即可看到图表。

### 在线预览

访问 [Mermaid Live Editor](https://mermaid.live/) 粘贴代码即可预览。

### 图表说明

1. **图1**：最简化的调用关系，适合快速理解
2. **图2**：详细的启动流程，展示4个阶段和所有实例
3. **图3**：组件依赖关系，展示共享组件的使用
4. **图4**：时序图，展示线程启动的时间顺序
5. **图5**：完整架构图，展示运行时的交互关系
6. **图6**：状态图，展示启动过程的状态转换
7. **图7**：代码级别的实例化流程，包含循环

### 颜色说明

- 🟥 红色：入口/主要流程
- 🟦 蓝色：共享组件（RequestChannel、ConnectionQuotas、MemoryPool）
- 🟩 绿色：Processor（Sub Reactors）
- 🟪 紫色：Acceptor（Main Reactor）
- 🟨 黄色：Handler（业务处理器）

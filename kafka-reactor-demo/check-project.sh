#!/bin/bash

# Kafka Multi-Reactor 项目完整性检查脚本

echo "========================================"
echo "Kafka Multi-Reactor 项目完整性检查"
echo "========================================"
echo ""

PROJECT_ROOT="src/main/java/com/kafka/reactor"

# 检查项目根目录
if [ ! -d "$PROJECT_ROOT" ]; then
    echo "❌ 错误: 找不到项目根目录 $PROJECT_ROOT"
    echo "   请在 kafka-reactor-demo 目录下运行此脚本"
    exit 1
fi

echo "✅ 项目根目录存在: $PROJECT_ROOT"
echo ""

# 定义所有需要的文件
declare -A FILES
FILES["common/MemoryPool.java"]="内存池接口"
FILES["common/SimpleMemoryPool.java"]="CAS内存池实现"
FILES["network/Acceptor.java"]="Main Reactor"
FILES["network/Processor.java"]="Sub Reactor"
FILES["network/KafkaChannel.java"]="连接封装"
FILES["network/KafkaSelector.java"]="NIO Selector封装"
FILES["network/NetworkReceive.java"]="网络接收"
FILES["network/NetworkSend.java"]="网络发送"
FILES["network/RequestChannel.java"]="请求响应队列"
FILES["network/TransportLayer.java"]="SocketChannel封装"
FILES["quota/ConnectionQuotas.java"]="连接配额管理"
FILES["server/SocketServer.java"]="主服务器"
FILES["server/RequestHandler.java"]="业务处理线程"

# 检查每个文件
MISSING_COUNT=0
TOTAL_COUNT=0

echo "检查核心类文件..."
echo "-----------------------------------"

for file in "${!FILES[@]}"; do
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    FILEPATH="$PROJECT_ROOT/$file"
    DESC="${FILES[$file]}"

    if [ -f "$FILEPATH" ]; then
        echo "✅ $file - $DESC"
    else
        echo "❌ 缺失: $file - $DESC"
        MISSING_COUNT=$((MISSING_COUNT + 1))
    fi
done

echo ""
echo "-----------------------------------"
echo "检查结果统计:"
echo "-----------------------------------"
echo "总共需要: $TOTAL_COUNT 个文件"
echo "已存在: $((TOTAL_COUNT - MISSING_COUNT)) 个文件"
echo "缺失: $MISSING_COUNT 个文件"
echo ""

# 检查关键方法
echo "-----------------------------------"
echo "检查关键修复 (NPE修复):"
echo "-----------------------------------"

check_method() {
    local file=$1
    local method=$2
    local desc=$3

    if [ -f "$PROJECT_ROOT/$file" ]; then
        if grep -q "$method" "$PROJECT_ROOT/$file"; then
            echo "✅ $file 包含 $method - $desc"
            return 0
        else
            echo "❌ $file 缺少 $method - $desc"
            return 1
        fi
    else
        echo "⚠️  文件不存在: $file"
        return 1
    fi
}

check_method "network/KafkaChannel.java" "maybeCompleteSend" "修复NPE的关键方法"
check_method "network/KafkaChannel.java" "hasSend" "检查send是否存在"
check_method "network/KafkaSelector.java" "channel.maybeCompleteSend" "正确的write实现"
check_method "network/Processor.java" "accept(SocketChannel socketChannel, boolean mayBlock)" "带mayBlock参数的accept"

echo ""

# 最终结果
echo "========================================"
if [ $MISSING_COUNT -eq 0 ]; then
    echo "✅ 项目完整! 所有 $TOTAL_COUNT 个文件都存在"
    echo "========================================"
    echo ""
    echo "可以运行以下命令测试编译:"
    echo "  mvn clean compile"
    exit 0
else
    echo "❌ 项目不完整! 缺失 $MISSING_COUNT 个文件"
    echo "========================================"
    echo ""
    echo "请从以下位置复制缺失的文件:"
    echo "  /home/user/kafka/kafka-reactor-demo/src/main/java/com/kafka/reactor/"
    exit 1
fi

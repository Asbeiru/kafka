# 知识库问答消息数据分析方案

## 一、核心分析指标

### 1. 意图识别准确率相关指标

#### 1.1 知识库命中率
```sql
-- 机器人回复中有knowledge_id的比例
SELECT
    COUNT(CASE WHEN knowledge_id IS NOT NULL AND knowledge_id != -1 THEN 1 END) * 100.0 / COUNT(*) as hit_rate
FROM im_message
WHERE from_user = 0
  AND autoreply = 1
  AND createtime >= 1731826800000  -- 2025-11-17 15:00:00 对应的毫秒时间戳
```

#### 1.2 答案类型分布
```sql
-- 不同answer_type的分布情况
SELECT
    answer_type,
    CASE
        WHEN answer_type = -1 THEN '历史数据'
        WHEN answer_type = 0 THEN '访客问题'
        WHEN answer_type BETWEEN 1 AND 5 THEN CONCAT('机器人答案类型', answer_type)
        WHEN answer_type >= 10 THEN '扩展字段'
    END as type_desc,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM im_message
WHERE from_user = 0
  AND createtime >= 1731826800000
GROUP BY answer_type
ORDER BY count DESC
```

#### 1.3 高频意图分布（TOP知识点）
```sql
-- 最常被匹配的知识点
SELECT
    knowledge_id,
    COUNT(*) as match_count,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) as positive_count,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) as negative_count
FROM im_message
WHERE from_user = 0
  AND knowledge_id IS NOT NULL
  AND knowledge_id != -1
  AND createtime >= 1731826800000
GROUP BY knowledge_id
ORDER BY match_count DESC
LIMIT 20
```

### 2. 用户满意度指标

#### 2.1 整体满意度
```sql
-- 评价率和满意度
SELECT
    COUNT(CASE WHEN evaluation != 0 THEN 1 END) as evaluated_count,
    COUNT(*) as total_count,
    COUNT(CASE WHEN evaluation != 0 THEN 1 END) * 100.0 / COUNT(*) as evaluation_rate,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN evaluation != 0 THEN 1 END), 0) as satisfaction_rate,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN evaluation != 0 THEN 1 END), 0) as dissatisfaction_rate
FROM im_message
WHERE from_user = 0
  AND autoreply = 1
  AND createtime >= 1731826800000
```

#### 2.2 不同答案类型的满意度对比
```sql
-- 各答案类型的满意度
SELECT
    answer_type,
    COUNT(*) as total,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) as satisfied,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) as dissatisfied,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN evaluation != 0 THEN 1 END), 0) as satisfaction_rate
FROM im_message
WHERE from_user = 0
  AND createtime >= 1731826800000
  AND evaluation != 0
GROUP BY answer_type
ORDER BY total DESC
```

#### 2.3 知识点满意度排名
```sql
-- 满意度最低的知识点（需要优化）
SELECT
    knowledge_id,
    COUNT(*) as total_use,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) as satisfied,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) as dissatisfied,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN evaluation != 0 THEN 1 END), 0) as dissatisfaction_rate
FROM im_message
WHERE from_user = 0
  AND knowledge_id IS NOT NULL
  AND knowledge_id != -1
  AND createtime >= 1731826800000
  AND evaluation != 0
GROUP BY knowledge_id
HAVING COUNT(CASE WHEN evaluation != 0 THEN 1 END) >= 5  -- 至少5次评价
ORDER BY dissatisfaction_rate DESC
LIMIT 20
```

### 3. 会话交互指标

#### 3.1 会话基本统计
```sql
-- 会话数量、消息量、活跃用户
SELECT
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT user_id) as active_user_count,
    COUNT(*) as total_message_count,
    COUNT(*) * 1.0 / COUNT(DISTINCT session_id) as avg_messages_per_session
FROM im_message
WHERE createtime >= 1731826800000
```

#### 3.2 会话轮次分布
```sql
-- 每个会话的交互轮次
WITH session_rounds AS (
    SELECT
        session_id,
        COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_message_count,
        COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_message_count
    FROM im_message
    WHERE createtime >= 1731826800000
    GROUP BY session_id
)
SELECT
    CASE
        WHEN user_message_count = 1 THEN '1轮'
        WHEN user_message_count BETWEEN 2 AND 3 THEN '2-3轮'
        WHEN user_message_count BETWEEN 4 AND 5 THEN '4-5轮'
        WHEN user_message_count BETWEEN 6 AND 10 THEN '6-10轮'
        ELSE '10轮以上'
    END as round_range,
    COUNT(*) as session_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
FROM session_rounds
GROUP BY
    CASE
        WHEN user_message_count = 1 THEN '1轮'
        WHEN user_message_count BETWEEN 2 AND 3 THEN '2-3轮'
        WHEN user_message_count BETWEEN 4 AND 5 THEN '4-5轮'
        WHEN user_message_count BETWEEN 6 AND 10 THEN '6-10轮'
        ELSE '10轮以上'
    END
ORDER BY MIN(user_message_count)
```

#### 3.3 平均响应时间
```sql
-- 机器人响应时间分析
SELECT
    AVG(bot.createtime - user.createtime) as avg_response_ms,
    MIN(bot.createtime - user.createtime) as min_response_ms,
    MAX(bot.createtime - user.createtime) as max_response_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY bot.createtime - user.createtime) as median_response_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY bot.createtime - user.createtime) as p95_response_ms
FROM im_message user
JOIN im_message bot ON bot.related_message_id = user.id
WHERE user.from_user = 1
  AND bot.from_user = 0
  AND user.createtime >= 1731826800000
```

#### 3.4 一次解决率
```sql
-- 单轮会话占比（用户只问一次就结束）
SELECT
    COUNT(CASE WHEN user_message_count = 1 THEN 1 END) as single_round_sessions,
    COUNT(*) as total_sessions,
    COUNT(CASE WHEN user_message_count = 1 THEN 1 END) * 100.0 / COUNT(*) as single_round_rate
FROM (
    SELECT
        session_id,
        COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_message_count
    FROM im_message
    WHERE createtime >= 1731826800000
    GROUP BY session_id
) t
```

### 4. 问题分析指标

#### 4.1 高频问题内容
```sql
-- 用户问题频次统计
SELECT
    content,
    COUNT(*) as ask_count,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT user_id) as user_count
FROM im_message
WHERE from_user = 1
  AND createtime >= 1731826800000
GROUP BY content
ORDER BY ask_count DESC
LIMIT 50
```

#### 4.2 未匹配知识库的问题
```sql
-- 没有knowledge_id的用户问题（可能需要补充知识库）
SELECT
    user_msg.content,
    COUNT(*) as count
FROM im_message user_msg
LEFT JOIN im_message bot_msg ON bot_msg.related_message_id = user_msg.id
WHERE user_msg.from_user = 1
  AND user_msg.createtime >= 1731826800000
  AND (bot_msg.knowledge_id IS NULL OR bot_msg.knowledge_id = -1)
GROUP BY user_msg.content
ORDER BY count DESC
LIMIT 50
```

#### 4.3 用户重复提问分析
```sql
-- 同一会话中用户重复问题（可能表示首次回答不满意）
WITH user_questions AS (
    SELECT
        session_id,
        user_id,
        content,
        ROW_NUMBER() OVER (PARTITION BY session_id, content ORDER BY createtime) as question_seq
    FROM im_message
    WHERE from_user = 1
      AND createtime >= 1731826800000
)
SELECT
    content,
    COUNT(DISTINCT session_id) as repeat_session_count
FROM user_questions
WHERE question_seq > 1
GROUP BY content
ORDER BY repeat_session_count DESC
LIMIT 20
```

### 5. 时间趋势分析

#### 5.1 每小时消息量趋势
```sql
-- 按小时统计消息量
SELECT
    FROM_UNIXTIME(createtime/1000, '%Y-%m-%d %H:00:00') as hour,
    COUNT(*) as message_count,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT user_id) as user_count
FROM im_message
WHERE createtime >= 1731826800000
GROUP BY FROM_UNIXTIME(createtime/1000, '%Y-%m-%d %H:00:00')
ORDER BY hour
```

#### 5.2 满意度时间趋势
```sql
-- 每小时满意度变化
SELECT
    FROM_UNIXTIME(createtime/1000, '%Y-%m-%d %H:00:00') as hour,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN evaluation != 0 THEN 1 END), 0) as satisfaction_rate,
    COUNT(CASE WHEN evaluation != 0 THEN 1 END) as evaluation_count
FROM im_message
WHERE from_user = 0
  AND createtime >= 1731826800000
GROUP BY FROM_UNIXTIME(createtime/1000, '%Y-%m-%d %H:00:00')
ORDER BY hour
```

### 6. 质量问题监控

#### 6.1 异常响应检测
```sql
-- 响应时间异常（超过5秒）
SELECT
    user.session_id,
    user.content as question,
    bot.content as answer,
    (bot.createtime - user.createtime) as response_ms
FROM im_message user
JOIN im_message bot ON bot.related_message_id = user.id
WHERE user.from_user = 1
  AND bot.from_user = 0
  AND user.createtime >= 1731826800000
  AND (bot.createtime - user.createtime) > 5000
ORDER BY response_ms DESC
```

#### 6.2 连续不满意会话
```sql
-- 同一会话中多次不满意评价
SELECT
    session_id,
    COUNT(CASE WHEN evaluation = 2 THEN 1 END) as dissatisfied_count,
    COUNT(CASE WHEN evaluation = 1 THEN 1 END) as satisfied_count
FROM im_message
WHERE from_user = 0
  AND createtime >= 1731826800000
  AND evaluation != 0
GROUP BY session_id
HAVING COUNT(CASE WHEN evaluation = 2 THEN 1 END) >= 2
ORDER BY dissatisfied_count DESC
```

## 二、综合分析建议

### 1. 意图识别准确率评估
- **知识库覆盖率**：knowledge_id不为空的比例
- **答案类型质量**：不同answer_type的满意度对比
- **未识别问题**：需要补充到知识库的高频问题

### 2. 用户体验优化
- **快速解决率**：单轮会话占比
- **重复提问率**：同一问题在会话中出现多次
- **评价分布**：满意/不满意的比例和趋势

### 3. 知识库优化方向
- **高频低满意度知识点**：需要优化答案内容
- **未匹配问题**：需要新增知识点
- **重复提问**：答案可能不够清晰

### 4. 性能监控
- **响应时间**：平均值、P95、P99
- **高峰时段**：消息量分布
- **异常检测**：超长响应时间

## 三、时间戳转换说明

您的createtime字段使用毫秒时间戳，转换方法：
- **2025-11-17 15:00:00** 对应的毫秒时间戳需要根据时区计算
- SQL中转换：`FROM_UNIXTIME(createtime/1000)`
- 如果是UTC+8时区，2025-11-17 15:00:00 对应约 **1731826800000** 毫秒

示例数据中的时间戳 1763024625114 对应 2025-11-13左右，请根据实际时区调整。

## 四、关键指标汇总表

| 指标类别 | 核心指标 | 计算方法 | 参考目标 |
|---------|---------|---------|---------|
| 意图识别 | 知识库命中率 | 有knowledge_id的回复/总回复数 | >90% |
| 意图识别 | 答案类型覆盖 | answer_type 1-5的占比 | >85% |
| 用户满意度 | 评价率 | 有评价的消息/总回复数 | >20% |
| 用户满意度 | 满意度 | 满意评价/总评价数 | >80% |
| 交互质量 | 一次解决率 | 单轮会话/总会话数 | >60% |
| 交互质量 | 平均轮次 | 总消息数/会话数 | <4轮 |
| 性能 | 平均响应时间 | 回复时间-问题时间 | <1000ms |
| 性能 | P95响应时间 | 95分位响应时间 | <2000ms |

## 五、分析流程建议

1. **日常监控**（每日）
   - 消息总量、会话数、活跃用户
   - 满意度、评价率
   - 平均响应时间

2. **周度分析**（每周）
   - 知识库命中率趋势
   - 高频问题TOP20
   - 低满意度知识点

3. **月度优化**（每月）
   - 未匹配问题汇总
   - 重复提问分析
   - 答案类型效果对比
   - 知识库优化建议

4. **专项分析**（按需）
   - 特定时间段问题
   - 特定知识点深度分析
   - 用户行为画像

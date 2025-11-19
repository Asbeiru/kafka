-- ============================================
-- 用户交互统计分析 SQL
-- 分析时间范围：2025-11-17 15:00:00 之后
-- ============================================

-- 时间戳常量（根据实际时区调整）
-- 2025-11-17 15:00:00 (UTC+8) = 1731826800000 毫秒
SET @start_time = 1731826800000;


-- ============================================
-- 1. 用户交互整体概览
-- ============================================
SELECT
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(*) as total_messages,
    COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_messages,
    COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_messages,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_messages_per_user,
    ROUND(COUNT(DISTINCT session_id) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_sessions_per_user
FROM im_message
WHERE createtime >= @start_time;


-- ============================================
-- 2. 按用户维度统计（每个用户的交互情况）
-- ============================================
SELECT
    user_id,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(*) as total_messages,
    COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_messages,
    COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_messages,
    MIN(createtime) as first_interaction_time,
    MAX(createtime) as last_interaction_time,
    ROUND((MAX(createtime) - MIN(createtime)) / 1000.0, 2) as total_duration_seconds,
    FROM_UNIXTIME(MIN(createtime)/1000) as first_interaction_datetime,
    FROM_UNIXTIME(MAX(createtime)/1000) as last_interaction_datetime
FROM im_message
WHERE createtime >= @start_time
GROUP BY user_id
ORDER BY total_messages DESC;


-- ============================================
-- 3. 用户交互消息次数分布
-- ============================================
WITH user_stats AS (
    SELECT
        user_id,
        COUNT(*) as message_count
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY user_id
)
SELECT
    CASE
        WHEN message_count = 1 THEN '1条消息'
        WHEN message_count BETWEEN 2 AND 5 THEN '2-5条消息'
        WHEN message_count BETWEEN 6 AND 10 THEN '6-10条消息'
        WHEN message_count BETWEEN 11 AND 20 THEN '11-20条消息'
        WHEN message_count BETWEEN 21 AND 50 THEN '21-50条消息'
        ELSE '50条以上'
    END as message_range,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM user_stats
GROUP BY
    CASE
        WHEN message_count = 1 THEN '1条消息'
        WHEN message_count BETWEEN 2 AND 5 THEN '2-5条消息'
        WHEN message_count BETWEEN 6 AND 10 THEN '6-10条消息'
        WHEN message_count BETWEEN 11 AND 20 THEN '11-20条消息'
        WHEN message_count BETWEEN 21 AND 50 THEN '21-50条消息'
        ELSE '50条以上'
    END
ORDER BY MIN(message_count);


-- ============================================
-- 4. 按会话维度统计（每个会话的交互情况）
-- ============================================
SELECT
    session_id,
    user_id,
    COUNT(*) as total_messages,
    COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_messages,
    COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_messages,
    MIN(createtime) as session_start_time,
    MAX(createtime) as session_end_time,
    ROUND((MAX(createtime) - MIN(createtime)) / 1000.0, 2) as session_duration_seconds,
    ROUND((MAX(createtime) - MIN(createtime)) / 60000.0, 2) as session_duration_minutes,
    FROM_UNIXTIME(MIN(createtime)/1000) as session_start_datetime,
    FROM_UNIXTIME(MAX(createtime)/1000) as session_end_datetime
FROM im_message
WHERE createtime >= @start_time
GROUP BY session_id, user_id
ORDER BY session_duration_seconds DESC;


-- ============================================
-- 5. 会话时长分布
-- ============================================
WITH session_duration AS (
    SELECT
        session_id,
        (MAX(createtime) - MIN(createtime)) / 1000.0 as duration_seconds
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY session_id
)
SELECT
    CASE
        WHEN duration_seconds < 10 THEN '0-10秒（极短）'
        WHEN duration_seconds < 30 THEN '10-30秒（短）'
        WHEN duration_seconds < 60 THEN '30-60秒（中）'
        WHEN duration_seconds < 180 THEN '1-3分钟（较长）'
        WHEN duration_seconds < 300 THEN '3-5分钟（长）'
        ELSE '5分钟以上（超长）'
    END as duration_range,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds
FROM session_duration
GROUP BY
    CASE
        WHEN duration_seconds < 10 THEN '0-10秒（极短）'
        WHEN duration_seconds < 30 THEN '10-30秒（短）'
        WHEN duration_seconds < 60 THEN '30-60秒（中）'
        WHEN duration_seconds < 180 THEN '1-3分钟（较长）'
        WHEN duration_seconds < 300 THEN '3-5分钟（长）'
        ELSE '5分钟以上（超长）'
    END
ORDER BY MIN(duration_seconds);


-- ============================================
-- 6. 平均交互统计汇总
-- ============================================
WITH session_stats AS (
    SELECT
        session_id,
        user_id,
        COUNT(*) as message_count,
        (MAX(createtime) - MIN(createtime)) / 1000.0 as duration_seconds,
        COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_msg_count,
        COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_msg_count
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY session_id, user_id
)
SELECT
    '会话统计' as metric_type,
    COUNT(*) as count,
    ROUND(AVG(message_count), 2) as avg_messages,
    ROUND(AVG(user_msg_count), 2) as avg_user_messages,
    ROUND(AVG(bot_msg_count), 2) as avg_bot_messages,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    ROUND(AVG(duration_seconds) / 60.0, 2) as avg_duration_minutes,
    ROUND(MIN(duration_seconds), 2) as min_duration_seconds,
    ROUND(MAX(duration_seconds), 2) as max_duration_seconds
FROM session_stats

UNION ALL

SELECT
    '用户统计' as metric_type,
    COUNT(DISTINCT user_id) as count,
    ROUND(AVG(message_count), 2) as avg_messages,
    ROUND(AVG(user_msg_count), 2) as avg_user_messages,
    ROUND(AVG(bot_msg_count), 2) as avg_bot_messages,
    ROUND(AVG(total_duration), 2) as avg_duration_seconds,
    ROUND(AVG(total_duration) / 60.0, 2) as avg_duration_minutes,
    ROUND(MIN(total_duration), 2) as min_duration_seconds,
    ROUND(MAX(total_duration), 2) as max_duration_seconds
FROM (
    SELECT
        user_id,
        SUM(message_count) as message_count,
        SUM(user_msg_count) as user_msg_count,
        SUM(bot_msg_count) as bot_msg_count,
        SUM(duration_seconds) as total_duration
    FROM session_stats
    GROUP BY user_id
) user_summary;


-- ============================================
-- 7. 用户活跃度分析（按会话数分组）
-- ============================================
WITH user_session_count AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) as session_count
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY user_id
)
SELECT
    CASE
        WHEN session_count = 1 THEN '1个会话（新用户/一次性）'
        WHEN session_count BETWEEN 2 AND 3 THEN '2-3个会话（低活跃）'
        WHEN session_count BETWEEN 4 AND 5 THEN '4-5个会话（中活跃）'
        WHEN session_count BETWEEN 6 AND 10 THEN '6-10个会话（高活跃）'
        ELSE '10个会话以上（超高活跃）'
    END as activity_level,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM user_session_count
GROUP BY
    CASE
        WHEN session_count = 1 THEN '1个会话（新用户/一次性）'
        WHEN session_count BETWEEN 2 AND 3 THEN '2-3个会话（低活跃）'
        WHEN session_count BETWEEN 4 AND 5 THEN '4-5个会话（中活跃）'
        WHEN session_count BETWEEN 6 AND 10 THEN '6-10个会话（高活跃）'
        ELSE '10个会话以上（超高活跃）'
    END
ORDER BY MIN(session_count);


-- ============================================
-- 8. 最活跃用户TOP20
-- ============================================
SELECT
    user_id,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(*) as total_messages,
    COUNT(CASE WHEN from_user = 1 THEN 1 END) as user_messages,
    COUNT(CASE WHEN from_user = 0 THEN 1 END) as bot_messages,
    ROUND((MAX(createtime) - MIN(createtime)) / 60000.0, 2) as total_duration_minutes,
    COUNT(DISTINCT DATE(FROM_UNIXTIME(createtime/1000))) as active_days,
    FROM_UNIXTIME(MIN(createtime)/1000) as first_interaction,
    FROM_UNIXTIME(MAX(createtime)/1000) as last_interaction
FROM im_message
WHERE createtime >= @start_time
GROUP BY user_id
ORDER BY total_messages DESC
LIMIT 20;


-- ============================================
-- 9. 用户每日交互趋势
-- ============================================
SELECT
    DATE(FROM_UNIXTIME(createtime/1000)) as interaction_date,
    COUNT(DISTINCT user_id) as daily_active_users,
    COUNT(DISTINCT session_id) as daily_sessions,
    COUNT(*) as daily_messages,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_messages_per_user,
    ROUND(COUNT(DISTINCT session_id) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_sessions_per_user
FROM im_message
WHERE createtime >= @start_time
GROUP BY DATE(FROM_UNIXTIME(createtime/1000))
ORDER BY interaction_date;


-- ============================================
-- 10. 用户每小时交互趋势
-- ============================================
SELECT
    DATE_FORMAT(FROM_UNIXTIME(createtime/1000), '%Y-%m-%d %H:00:00') as hour,
    COUNT(DISTINCT user_id) as hourly_active_users,
    COUNT(DISTINCT session_id) as hourly_sessions,
    COUNT(*) as hourly_messages,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT user_id), 2) as avg_messages_per_user
FROM im_message
WHERE createtime >= @start_time
GROUP BY DATE_FORMAT(FROM_UNIXTIME(createtime/1000), '%Y-%m-%d %H:00:00')
ORDER BY hour;


-- ============================================
-- 11. 用户留存分析（按首次交互日期分组）
-- ============================================
WITH user_first_interaction AS (
    SELECT
        user_id,
        DATE(FROM_UNIXTIME(MIN(createtime)/1000)) as first_date
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY user_id
),
user_activity AS (
    SELECT
        m.user_id,
        u.first_date,
        DATE(FROM_UNIXTIME(m.createtime/1000)) as activity_date
    FROM im_message m
    JOIN user_first_interaction u ON m.user_id = u.user_id
    WHERE m.createtime >= @start_time
)
SELECT
    first_date,
    COUNT(DISTINCT user_id) as new_users,
    COUNT(DISTINCT CASE WHEN activity_date = first_date THEN user_id END) as day0_active,
    COUNT(DISTINCT CASE WHEN activity_date = DATE_ADD(first_date, INTERVAL 1 DAY) THEN user_id END) as day1_active,
    COUNT(DISTINCT CASE WHEN activity_date = DATE_ADD(first_date, INTERVAL 2 DAY) THEN user_id END) as day2_active,
    COUNT(DISTINCT CASE WHEN activity_date = DATE_ADD(first_date, INTERVAL 3 DAY) THEN user_id END) as day3_active,
    COUNT(DISTINCT CASE WHEN activity_date = DATE_ADD(first_date, INTERVAL 7 DAY) THEN user_id END) as day7_active
FROM user_activity
GROUP BY first_date
ORDER BY first_date;


-- ============================================
-- 12. 会话间隔分析（用户连续会话的时间间隔）
-- ============================================
WITH session_times AS (
    SELECT
        user_id,
        session_id,
        MIN(createtime) as session_start,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY MIN(createtime)) as session_seq
    FROM im_message
    WHERE createtime >= @start_time
    GROUP BY user_id, session_id
),
session_gaps AS (
    SELECT
        s1.user_id,
        s1.session_id as session1,
        s2.session_id as session2,
        (s2.session_start - s1.session_start) / 3600000.0 as gap_hours
    FROM session_times s1
    JOIN session_times s2
        ON s1.user_id = s2.user_id
        AND s2.session_seq = s1.session_seq + 1
)
SELECT
    CASE
        WHEN gap_hours < 1 THEN '1小时内'
        WHEN gap_hours < 24 THEN '1-24小时'
        WHEN gap_hours < 72 THEN '1-3天'
        WHEN gap_hours < 168 THEN '3-7天'
        ELSE '7天以上'
    END as gap_range,
    COUNT(*) as gap_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(gap_hours), 2) as avg_gap_hours
FROM session_gaps
GROUP BY
    CASE
        WHEN gap_hours < 1 THEN '1小时内'
        WHEN gap_hours < 24 THEN '1-24小时'
        WHEN gap_hours < 72 THEN '1-3天'
        WHEN gap_hours < 168 THEN '3-7天'
        ELSE '7天以上'
    END
ORDER BY MIN(gap_hours);

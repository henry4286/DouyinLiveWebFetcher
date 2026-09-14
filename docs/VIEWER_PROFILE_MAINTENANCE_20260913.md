# 观众资料输出维护

2026-09-13，独立采集仓库分支 `codex/viewer-profile-fields`。

## 目的与交付

为懂小播后续的观众识别、上下文回应和互动复盘提供完整的资料观察。旧协议解析器已支持头像、displayId、粉丝数、关注数，原来的输出整理未保留这些字段。本次集中修改 `collector_events.actor_fields()`，通过既有回调和 NDJSON 输出，不另建认证、任务、上传或数据库体系。

`actor.observedProfile` 新增以下可选字段：

| 字段 | 来源 | 输出规则 |
| --- | --- | --- |
| displayId | User.display_id | 保留原字符串，1–256 字符，无控制字符或空白；不是 UID 的替代键 |
| avatarUrl | User.avatar_thumb / medium / large 的 url_list_list | 按此顺序取首个有效 HTTP(S) 地址，最多 4096 字符，不含账号密码；只保留地址，不下载图片 |
| followerCount | User.follow_info.follower_count | 正整数且不超过 JS 安全整数范围；缺失和 protobuf 默认 0 均不输出 |
| followingCount | User.follow_info.following_count | 同上 |

已有 followStatusRaw、payGradeLevel、fansClubLevel、fansClubStatusRaw、fansClubAnchorId 保留。UID 仍用十进制字符串保留，0/111111 不升级为可用身份；`sourceUserId=null` 和 `quality.actorId=unavailable` 不变。资料观察和按样本核验的 TikHub 资料对应，应分开记录，不能因 3 个样本通过就自动认证所有观众。

评论、进场、礼物、点赞、社交、粉丝团均通过同一 actor 函数获得新字段。原始礼物计数、来源、事件 ID 和时间质量继续保留，不在这次字段维护中更改计数语义。

## 消费接口边界

根 schemaVersion 仍为 `douyin-web-collector-event-v1`，只扩展现有可选 observedProfile。使用严格字段白名单的消费者需要接受这四个新增键，不能宣称未更新的旧客户端一定兼容。本轮维护到本仓库回调/NDJSON 出口为止；没有恢复归档业务服务或旧数据库，也没有把这批事件接入懂小播正式后端。

消费方遇到缺失计数，应显示未知而不是 0；头像、抖音号和昵称都不能单独作为观众唯一身份。等级不是现金金额，互动行为不是已验证的长期偏好。当前新项目消费逻辑应进入其 Agent 模块与新契约，独立评估用途授权。

## 验证

```bash
cd /Users/apple/Desktop/DouyinLiveWebFetcher
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/probe_viewer_profile.py 170664225650 --seconds 30 --output build/viewer-profile-20260913.json
```

46 项测试通过。新增测试将六类 protobuf 消息封装成 gzip WebSocket 帧，经实际路由、actor 整理、NDJSON 写出和 JSON 读回，验证新旧字段、uint64 ID、来源与礼物原始字段。另检查默认零值、超范围计数、占位 ID、无效头像地址、字符串保真及回退头像。

真实探针只保存每类事件字段覆盖计数和原字段/输出相等次数，不落盘评论、昵称或账号值。不调用 TikHub、不发送消息、不启动旧业务链路；限时结束后停止。实测结果见被忽略的 `build/viewer-profile-20260913.json`，不能把合成测试当作礼物真实验收。

本次 30 秒真实采集已完成：44 条评论、75 条进场、14 条点赞、13 条社交及 48 条统计消息，无错误，正常停止。四个新增字段共 574 次有效原始观察，经事件输出与 NDJSON 读回后 574/574 相等，差异 0。评论的 displayId/头像/关注数各 44 条、非零粉丝数 42 条；进场 displayId 73 条、头像 75 条、非零粉丝数/关注数各 72 条。原有等级、关注状态和粉丝团字段继续输出。缺失/默认零值没有补造。本次仍没有礼物消息，礼物字段保真只有合成链路验证。

## Java 与签名服务的选型

用户已明确：若新抓取更安全稳定，可以替换，不因迁移成本锁定旧引擎。本次字段补齐不代表永久选定 Python。

- 旧版本机生成签名；Java SDK 调用外部 HTTP 服务获取带签名 WSS 地址，然后直接连接抖音。签名服务负责连接凭据，不自动替代采集、解析、重连和事件可靠交付。
- 拆开签名可方便集中更新和多客户端复用；也引入服务可用性、签名时效和鉴权依赖。它不天然证明平台风控风险更低，也不代表官方授权通道。
- 本次对比用的是旧引擎生成签名再提供给 Java；作者独立托管签名服务尚未测。因此只证明这一共同来源下的采集/解析能力，不能评判作者服务是否更稳定。
- 可见代码的当前差异：旧引擎已有压缩帧/解压后大小限制；Java `GzipUtil` 没有对应上限。Java `connectBlocking()` 未指定握手时限，SDK 无自动重连；旧引擎本身也未内置完整自动重连，本次不能借用归档业务监管器宣称旧版已具备生产保障。
- Java 礼物聚合已在独立合成测试中复现重复结算、超时后再结算重复输出、groupId 缺失相互覆盖和乱序计数变小。旧版保留原始计数但同样没有完成真实礼物计数验收。

现有证据不支持“Java 更安全稳定”或“旧版全面更安全”的结论。后续选择应看实际签名服务下的连接成功率、长跑断流恢复、消息缺口/重复、礼物计数与资源使用，而不只看编程语言或多一层服务。

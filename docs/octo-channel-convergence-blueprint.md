# Octo → 共享 channel.Channel 引擎收敛实现蓝图

> 状态：**实现蓝图（未动手）**。基于 `main` @ 当前 HEAD 重新评估，取代已过期的 PR #48
> (`refactor/octo-channel-engine`，基线落后 main 203 个 commit)。
>
> 决策已定：
> - WuKongIM `message_seq` → **编码进 `channel_card_message_id`**（`"<msgID>:<seq>"`），零 schema 改动。
> - 范围：**完整收敛**（迁移 + 适配层 + 接线 + 删并行栈），一次到位（产品未上线，清爽切换）。

---

## 0. 为什么 PR #48 不能直接复用

| PR #48 假设 | main 现状 |
|---|---|
| 引擎在顶层 `integrations/engine/` | 已在 `integrations/channel/engine/`（`Supervisor`/`Router`/`ResolverSet`/`ChatSession`） |
| octo 表 = migration 120 | = **149**（sync 时 120→149，由 `runOctoWebhookRenumberHook` 重编号） |
| 折叠迁移用 **128** | 128 已占用；当前最高 **153**，新迁移应为 **154** |
| `octo_chat` 约束只在折叠迁移维护 | 已有专门的 **153_issue_origin_type_union** 无条件维护 |
| Slack 功能与 Feishu 对齐 | Slack 又新增：BYO-app 安装、按安装 Socket Mode、typing 反应、`chat history/thread` |
| 引擎不做 group 过滤 | Router 现内建 dedup / group @bot 过滤 / 身份+成员校验 / `/issue` / run-debounce |

方向正确、代码作废。以下以 **Slack adapter（最新参考实现）** 为模板重写。

---

## 1. 数据层

### 1.1 新迁移 `154_octo_to_channel`

复用现有 fork 迁移套路（参考 149 的 renumber hook 说明、153 的“无条件全量替换”思路）。两条路径都要幂等：

**up（`154_octo_to_channel.up.sql`）**

1. 折叠 `octo_installation` → `channel_installation`（`channel_type='octo'`）：
   - `robot_id` → `config->>'app_id'`（通用路由键槽位，命中 `GetChannelInstallationByAppID` + `(channel_type, app_id)` 唯一索引，与 Slack 用 team_id 同法）。
   - `bot_token_encrypted`（bytea/secretbox）→ base64 后放进 `config` JSON。
   - `bot_name` / `api_url` / `ws_url` / `owner_uid` → 一并进 `config` JSON。
   - `agent_id` / `workspace_id` / `installer_user_id` / `status` / `installed_at` → 通用列。
   - lease 列（`ws_lease_token` / `ws_lease_expires_at`）→ `channel_installation` 的对应通用 lease 列。
2. 折叠 `octo_user_binding` → `channel_user_binding`（`channel_type='octo'`，`channel_user_id = octo_uid`）。
3. 折叠 `octo_chat_session_binding` → `channel_chat_session_binding`：
   - `octo_channel_id` → `channel_chat_id`（p2p：直接用；group：这里没有 thread 概念，见 §2.4）。
   - `octo_channel_type`(int16 1/2/5) → `chat_type`('p2p'/'group')，`config` 存原始 `octo_channel_type`（供 outbound 的 WuKongIM `ChannelType` 用）。
4. 折叠 `octo_inbound_dedup` → `channel_inbound_message_dedup`。
5. 折叠 `octo_inbound_audit` → `channel_inbound_audit`。
6. 折叠 `octo_outbound_message` → `channel_outbound_card_message`：
   - `channel_card_message_id = octo_message_id || ':' || octo_message_seq`（编码 seq）。
7. 折叠 `octo_binding_token` → `channel_binding_token`。
8. `DROP TABLE octo_*`（7 张）。
9. `issue.origin_type` 约束：153 已含 `octo_chat`，**无需再动**。

**down（`154_octo_to_channel.down.sql`）**：重建 7 张 octo_* 表并从 `channel_*`（`channel_type='octo'`）反向回填，`card_message_id` 按 `:` 拆回 `octo_message_id`/`octo_message_seq`。

**与 renumber hook 的关系**：154 是 sync 之后的“真实”新增迁移，`runOctoWebhookRenumberHook` 只处理 120–123→149–152，不涉及 154。**升级路径**（老库有 octo_* 数据）走 INSERT…SELECT + DROP；**全新安装**（149 被 hook 跳过 SQL、octo_* 从未建出）时，154 的 INSERT…SELECT 命中 0 行、DROP 用 `IF EXISTS`，天然幂等。CI 的 `pgvector/pgvector:pg17` 覆盖迁移应用。

> ⚠️ 落地前必须从**真实 base** 验证升级+全新两条路径（见 [[migration-renumber-upstream-sync]] 记录的坑：upgrade crash / skipped-constraint）。

### 1.2 删除 SQL 与生成代码

- 删 `server/pkg/db/queries/octo.sql`（30 条查询）。
- `make sqlc` 重生成：删除 `server/pkg/db/generated/octo.sql.go` 与 `models.go` 中 7 个 `Octo*` struct。
- Octo 包全部改用通用 `channel_*` 查询（`GetChannelInstallationByAppID`、`Claim/Mark/ReleaseChannelInboundDedup`、`*ChannelUserBinding`、`*ChannelChatSessionBinding`、`*ChannelOutboundCardMessage`、`*ChannelBindingToken`、`RecordChannelInboundDrop`、`ListAllActiveChannelInstallations`、`Acquire/ReleaseChannelWSLease` 等，均已存在）。

---

## 2. 适配层（`server/internal/integrations/octo/`）

### 2.1 新增 `config.go`（模板：slack/config.go）

```go
type installConfig struct {
    AppID             string `json:"app_id"`      // = robot_id，路由键
    BotName           string `json:"bot_name,omitempty"`
    OwnerUID          string `json:"owner_uid,omitempty"`
    APIURL            string `json:"api_url"`
    WSURL             string `json:"ws_url,omitempty"`
    BotTokenEncrypted string `json:"bot_token_encrypted"`
}
```
- `decodeCredentials(raw, decrypt)` / `DecodePublicConfig(raw)`（供 handler 出 `robot_id`/`bot_name`）。
- 复用 slack 的 `decryptToken` / `stripWhitespace` 模式（每 adapter 自带，lark/slack 均重复，符合既有约定）。

### 2.2 新增 `octo_channel.go`（模板：slack/slack_channel.go + feishu_channel.go）

`octoChannel` 实现 `channel.Channel`：
- `Type() → TypeOcto`（在本包定义 `const TypeOcto channel.Type = "octo"`）。
- `Capabilities() → CapText | CapMessageEdit`（WuKongIM 支持 edit；无 typing）。
- `Disconnect() → nil`（生命周期随 Connect，同 slack/feishu）。
- `Send(ctx, out)` → 走 `transport.HTTPClient.SendMessage`（引擎极少直接调 Send，Octo 主出站在 patcher，见 §2.5）。
- **`Connect(ctx)`**（关键，含 PR #48 第二 commit 的 bug 修复）：
  1. 解 config → 解密 bot token → `transport.NewHTTPClient(apiURL, token).Register(ctx,...)` 拿 `im_token`/`ws_url`/`robot_id`。
  2. `sock := transport.NewSocket(...)`，`OnMessage` 归一化成 `channel.InboundMessage` 交 `c.handler`。
  3. **`OnError` 写入一个 buffered `socketErr chan error`**。`Connect` 用 `select` 在 `ctx.Done()`（→nil）与 `<-socketErr`（→返回该 err）间阻塞。
     - 原因：WuKongIM socket 遇 terminal 错误（被踢 / 快速断连 / im_token 失效）会**停止内部重连**并让 manager 退出。若 `Connect` 只 `<-ctx.Done()`（现 connector.go 的写法），连接会静默死亡而 Supervisor 仍在续租 → bot 假死到重启。返回 err 让 Supervisor 在 backoff 下重建（重跑 Register 拿新 im_token）。
- `RegisterOcto(reg *channel.Registry, deps ChannelDeps)` + `newOctoFactory(deps)`（闭包 per-installation 解 config、建 `octoChannel`）。

归一化 `channelMessageFromOcto`（模板：`channelMessageFromLark`）：
- 复用现有 `mention_strip.go`（剥 bot @mention）与 `fresh_command.go`（解 `/new` → `ForceFresh`）。
- `Source.ChatType`：`ChannelDM(1)→p2p`，`ChannelGroup(2)/Topic(5)→group`。
- `AddressedToBot`：group 场景由 mention 判定（现 `hub.addressedToBot` 逻辑迁进来）。
- 原始 WuKongIM 字段（`octo_channel_type` 等）塞 `Raw`，供 resolver 读。

### 2.3 新增 `octo_resolvers.go`（模板：slack/resolvers.go）

`NewOctoResolverSet(q *db.Queries, tx engine.TxStarter, replier engine.OutboundReplier) engine.ResolverSet`：
- `Installation`：`GetChannelInstallationByAppID{ChannelType:"octo", AppID: raw.RobotID}`（Octo 单部署、单 app_id 即 robot_id，无 Slack 的 team 二次校验）。`Platform` 存 `db.ChannelInstallation` 行。
- `Identity`：`GetChannelUserBindingByUserID`（`ChannelUserID = octo_uid`）+ `GetMemberByUserAndWorkspace` 复检成员。Octo 无跨 app 复用需求，可省 `FindReusableChannelUserBinding`。
- `Dedup`：通用两阶段 `Claim/Mark/Release`（与 slack 逐字同构）。
- `Session`：`engine.NewChatSession(q, tx, TypeOcto, SessionTitles{Group:"Octo 群聊", Direct:"Octo 私聊", Fallback:"Octo 会话"})`。
- `Audit`：`RecordChannelInboundDrop`。
- `Replier`：见 §2.5（needs_binding / offline / archived）。
- `OriginType = "octo_chat"`。
- 无 `Typing`（WuKongIM 无 typing 语义）。

### 2.4 会话隔离键（`octoSessionRouting`）

Octo 目前**无 thread 概念**（不像 Slack 按 thread root 隔离），沿用现 `chat_service.go` 行为：`bindingKey = octo_channel_id`。`config` 存 `{octo_channel_type}`（outbound 需要原始 int16 类型）。`replyThread` 空。

### 2.5 改写 outbound / binding / client

- `outbound.go`（Patcher，模板：lark/channel_store.go 的 card 存取 + slack outbound）：
  - 读 `GetChannelChatSessionBindingBySession` + `GetChannelInstallation`（解 config 得 api_url/token/channel_type）。
  - 发送后写 `CreateChannelOutboundCardMessage{channel_card_message_id: msgID+":"+seq, channel_type:"octo", status:"final"}`。
  - 注：`EditMessage`/seq 读取路径**当前休眠**（业务层无调用者），编码进 card_id 仅为未来流式；先保持一次 `Send`。
- `binding_token.go`：`CreateChannelBindingToken` / `ConsumeChannelBindingToken` + `CreateChannelUserBinding`（in-tx），错误映射不变。
- `client.go`（`InstallationService`）：`UpsertChannelInstallationByAppID` 写 config（seal bot token 进 config）；`GetChannelInstallationInWorkspace` / `ListChannelInstallationsByWorkspace` / `SetChannelInstallationStatus`。`ErrRobotAlreadyBound` 改判 `(channel_type, app_id)` 唯一冲突。
- `outcome_replier.go`：改从 config 解 token/api_url；其余（mint token、DM 绑定链接、offline/archived 中文文案）不变。

### 2.6 删除并行栈

删 `hub.go`、`dispatcher.go`、`connector.go`、`chat_service.go`、`audit.go`（+ 对应 `_test.go`、`db_test.go` 中 octo_* fixtures）。`types.go` 精简为仍需的 `UID`/`ChannelType`/`InstallationStatus` 等。

---

## 3. 接线

### 3.1 `server/cmd/server/router.go`（模板：Slack 块）

`MULTICA_OCTO_SECRET_KEY` 分支内，改为注册到**已存在的**共享 `channelRegistry`/`channelRouter`：
```go
octoReplier := octo.NewOutcomeReplier(...)               // engine.OutboundReplier
channelRouter.Register(octo.TypeOcto,
    octo.NewOctoResolverSet(queries, pool, octoReplier))
octo.NewPatcher(queries, box.Open, octo.NewMessageSender(), slog.Default()).Register(bus)
octo.RegisterOcto(channelRegistry, octo.ChannelDeps{Decrypt: box.Open, Logger: slog.Default()})
```
删除 `octo.NewHub(...)` / `SetOutcomeReplier` / `NewConnectorFactory` / `&Dispatcher{}` / `NewChatSessionService` / `NewAuditLogger`。`h.OctoInstallations` / `h.OctoBindingTokens` / `h.OctoAPIBaseURL` 保留（handler 仍用）。

### 3.2 `server/cmd/server/main.go`

删 `OctoHub` 的 `go h.OctoHub.Run(sweepCtx)`（412–417）与 shutdown 的 `WaitWithTimeout`（535–542）。Octo 现由通用 `ChannelSupervisor` 驱动，无独立 Run/Wait。

### 3.3 `server/internal/handler/handler.go`

删 `OctoHub` 字段。

### 3.4 scheduler `jobs_octo_cleanup.go`

改用 `PurgeChannelInboundDedup` + `PurgeExpiredChannelBindingTokens`（顺带覆盖 Feishu/Slack）。

---

## 4. HTTP / 前端契约（保持 JSON 不变）

`handler/octo.go`：`octoInstallationToResponse` 改为入参 `db.ChannelInstallation`，通过 `octo.DecodePublicConfig(row.Config)` 取 `robot_id`(=app_id)/`bot_name`。响应 JSON 字段 **完全不变**（`robot_id`/`bot_name`/`installer_user_id`/…），前端 `packages/core/types/octo.ts` 与所有 UI **零改动**。

按 CLAUDE.md「API Compatibility」：新增/改动的 schema 需配 malformed-response 测试（此处响应形状不变，主要保证 config 解不出时降级为空串而非报错，仿 slack `DecodePublicConfig`）。

---

## 5. 测试

| 层 | 动作 |
|---|---|
| `octo_channel_test.go` | 新增：terminal `OnError` → Connect 返回 err（Supervisor 重建）；`ctx` 取消 → 返回 nil（race-clean，`-race`）。**这是 PR #48 回归测试的核心，必带。** |
| `octo_resolvers_test.go` | 新增：installation 路由 / 身份 / dedup / session / audit 走通用表。 |
| 迁移测试 | `cmd/migrate` 加 154 的升级+全新双路径回归（仿 `TestRunOctoWebhookRenumberHook*`）。 |
| 删除 | `hub_test.go` / `dispatcher_test.go` / `chat_service_test.go` 及 db_test 的 octo_* fixtures。 |
| handler | malformed config → 响应降级测试。 |

---

## 6. 落地顺序（3 个可独立验证的 commit）

1. **`refactor(octo): fold octo_* into channel_* (migration 154 + sqlc)`** —— 迁移 + 删 octo.sql + `make sqlc`。验证：迁移双路径、`go build`。
2. **`refactor(octo): implement channel.Channel + ResolverSet`** —— config.go / octo_channel.go / octo_resolvers.go + 改写 outbound/binding/client + 删并行栈 + handler 适配。验证：`go test ./...octo/...`、`-race`。
3. **`refactor(octo): wire onto shared supervisor; drop OctoHub`** —— router.go / main.go / handler.go / scheduler。验证：`make test`、`go vet`、`@multica/core` typecheck。

**收益**：Octo 免费继承 `/issue` + run-debounce + 未来引擎改进；`octo.Hub` 与 `engine.Supervisor` 的逐字重复消失，server 净减约 3k+ 行；不再有并行栈引发的 sync 冲突。

---

## 7. 与 PR #48 的关键差异清单（review 用）

1. 迁移号 128 → **154**；且必须与 renumber hook + 153 约束迁移共存（PR #48 无此约束）。
2. 引擎路径 `integrations/engine/` → **`integrations/channel/engine/`**。
3. seq 处理：PR #48 未明确 → 本方案**编码进 `channel_card_message_id`**（seq 路径当前休眠，零成本）。
4. 参考实现从 Feishu 升级为 **Slack**（BYO/per-install 模型更贴近，且 config-blob 路由键做法一致）。
5. `Connect` terminal-`OnError` 修复：**保留** PR #48 第二 commit 的核心修复。
6. Octo 无 thread / 无 typing / 无跨-app 身份复用 → resolver 比 Slack 简化。

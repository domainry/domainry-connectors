# 知识库 HTTP Connector

官方 Provider：`github.com/domainry/domainry-connectors/providers/knowledge_base/http_api`，身份为 `knowledge_base/http_api`。构造入口为 `httpapi.New(connector.Transport)`。

该包只负责外部协议；对话检索时机、模型上下文、来源展示和会话持久化由 Agent 负责。宿主负责连接、密钥、认证身份和网络策略，Provider 不读取环境、不持有 HTTP Client，也不依赖 Agent 或 Runtime 实现。

## 操作

| 操作 | 类型化输入 | 上游接口 |
| --- | --- | --- |
| `search` | `SearchInput{Query, TopK, ResultContent}` | `POST /v1/kb/search` |
| `fetch` | `FetchInput{DocID, ResultContent}` | `POST /v1/kb/fetch` |

均为同步只读操作，默认 30 秒超时，响应上限 512 KiB。`TopK` 默认 5，本地限制 1–20。`ResultContent` 留空返回内容，`metadata` 请求省略文本的结果。

两种操作统一使用 `ResultContent`，由 Provider 转换为各自的上游参数：search 发送 `result_content: "metadata"`；fetch 发送 `include_content: false` 和 `live.enabled: false`，同时关闭离线分块和实时内容。

输出 `Output{Provider, KBID, Result}` 中 `Result` 为完整上游 JSON。控制台没有展示响应字段契约，因此不猜测字段名；已存在的内容、来源、额外字段及大整数都保留。错误状态、无效 JSON、标量结果和超大响应不会作为成功结果返回。

2026-09-10 真实调用确认 HTTP 200 也可能表示业务失败。顶层 `err_code` 存在时必须是整数：`0` 为成功；已观察到的 `1004`（document not found）映射为 `knowledge_api.not_found`；其他非零值映射为 `knowledge_api.failed`，不猜测重试或授权语义。类型无效则为 `knowledge_api.response_invalid`。失败响应的 `err_msg` 和 `data` 不进入操作结果；成功 JSON 保持完整。

## 连接与权限

配置字段：`base_url`（必填，不设置默认服务商）、`team_id`、`kb_id`、可选 `permission_ids_by_user`。密钥字段：`api_key`，通过 SDK `SecretHeaders` 交由宿主注入 Bearer 鉴权。

每次调用要求可信的认证 Principal 与 Connection 工作区一致。`team_id`、`kb_id` 和文档权限只能来自宿主连接配置；operation payload 不接受这些字段。

`permission_ids_by_user` 为服务端维护的用户 ID 到精确权限 ID 数组的映射。Provider 根据 Principal.UserID 选择当前用户的权限；没有映射时仅查团队可见文档。search 和 fetch 使用相同规则。宿主必须限制连接配置的修改权限，不能让模型或普通浏览器调用者替换连接、Principal 或该映射。

## bcri 与验证

依据接入时读取的控制台中 bcri 详情：`team_id=1470194374940573696`，`kb_id=kb-3bbd8f1d3249`。该实例标识只用于部署配置，不写入 Provider 实现。

```sh
go test ./providers/knowledge_base/http_api
go run ./scripts/verify_provider_release --provider knowledge_base/http_api --mode deterministic
```

确定性测试使用注入的 Transport fake，覆盖请求翻译、SDK/Catalog 身份、权限、内容保留及错误处理，包括 HTTP 200 业务失败。2026-09-10 已使用获准凭证完成成功空结果检索，不存在文档返回 HTTP 200 / `err_code: 1004`。随后从已登录的系统 Chrome 控制台确认推送 / 删除契约，专门创建的合成文档已推送并索引；真实 search 的 `/data/hits` 和 fetch 的 `/data/chunks` 均取得 4 个片段。首次 fetch 可能为 `PENDING` 且 chunks 为空，HTTP 成功不等于索引完成。非空文档字段映射由 Agent 配置并验证，Connector 继续保留完整成功 JSON。真实私有文档 ACL 尚未验收。此前 GET 管理详情探测返回 401，不能据此推断已验证的文档推送能力不可用。

本次提供检索和全文读取，不包括文档推送、删除、索引同步任务或后台管理页面。

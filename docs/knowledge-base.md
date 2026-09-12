# 知识库 HTTP Connector

官方 Provider：`github.com/domainry/domainry-connectors/providers/knowledge_base/http_api`，身份为 `knowledge_base/http_api`。构造入口为 `httpapi.New(connector.Transport)`。

该包只负责外部协议；对话检索时机、模型上下文、来源展示和会话持久化由 Agent 负责。宿主负责连接、密钥、认证身份和网络策略，Provider 不读取环境、不持有 HTTP Client，也不依赖 Agent 或 Runtime 实现。

## 操作

| 操作 | 类型化输入 | 上游接口 |
| --- | --- | --- |
| `search` | `SearchInput{Query, TopK, ResultContent}` | `POST /v1/kb/search` |
| `fetch` | `FetchInput{DocID, ResultContent}` | `POST /v1/kb/fetch` |
| `put_document` | `PutDocumentInput{DocID, Filename, Content}` | `POST /v1/kb/kbs/{kb_id}/documents?doc_id=…&filename=…` |
| `document_status` | `DocumentInput{DocID}` | `POST /v1/kb/fetch`，关闭内容与 live |
| `delete_document` | `DocumentInput{DocID}` | `DELETE /v1/kb/kbs/{kb_id}/documents?doc_id=…` |

所有操作调用本身同步，默认 30 秒超时，响应上限 512 KiB；文档推送后的索引仍由上游异步处理。search / fetch / document_status 声明只读与自然幂等；put 为非幂等写入；delete 为同一固定文档的自然幂等写入。Provider 每次只发送一次请求，不实现自动重试、回执查询或补偿。`TopK` 默认 5，本地限制 1–20。`ResultContent` 留空返回内容，`metadata` 请求省略文本的结果。

search / fetch 统一使用 `ResultContent`，由 Provider 转换为各自的上游参数：search 发送 `result_content: "metadata"`；fetch 发送 `include_content: false` 和 `live.enabled: false`，同时关闭离线分块和实时内容。

输出 `Output{Provider, KBID, Result}` 中 `Result` 为完整上游 JSON。控制台没有展示响应字段契约，因此不猜测字段名；已存在的内容、来源、额外字段及大整数都保留。错误状态、无效 JSON、标量结果和超大响应不会作为成功结果返回。

put 的 `Content` 在 SDK JSON 中为 Base64，在实际 HTTP 中发送原始二进制与 `application/octet-stream`；按已读上游源码限制为 10 MiB。文件名只接受 ASCII 字母、数字、空格、点、下划线和连字符，以字母或数字开头，最多 255 字节，拒绝路径；文档写入 ID 同样以 ASCII 字母或数字开头，只含字母、数字、点、下划线和连字符，最多 128 字节。库 ID 固定来自 Connection，文档 ID 和文件名通过 URL 编码进入查询参数。put / delete 保留上游受理 JSON，不把 HTTP 成功解释成索引就绪或全部片段已清理。

`document_status` 返回 `DocumentStatusOutput{Provider, KBID, DocID, Exists, IndexStatus}`。读取实际 `/data/status`，保留 `PENDING`、`CHUNKED`、`INDEXED` 或未知状态，不带正文。只有明确的业务 `1004` 转为当前权限范围内 `Exists=false`；权限拒绝、网络故障、未知响应不能当成不存在。上游可能把无权限文档隐藏为不存在，因此该值单独不能证明全局删除。

2026-09-10 真实调用确认 HTTP 200 也可能表示业务失败。顶层 `err_code` 存在时必须是整数：`0` 为成功；已观察到的 `1004`（document not found）映射为 `knowledge_api.not_found`；其他非零值映射为 `knowledge_api.failed`，不猜测重试或授权语义。类型无效则为 `knowledge_api.response_invalid`。失败响应的 `err_msg` 和 `data` 不进入操作结果；成功 JSON 保持完整。

## 连接与权限

配置字段：`base_url`（必填，不设置默认服务商）、`team_id`、`kb_id`、可选 `permission_ids_by_user` 与 `document_permission_ids`。密钥字段：`api_key`，通过 SDK `SecretHeaders` 交由宿主注入 Bearer 鉴权。

每次调用要求可信的认证 Principal 与 Connection 工作区一致。`team_id`、`kb_id` 和文档权限只能来自宿主连接配置；operation payload 不接受这些字段。

`permission_ids_by_user` 为服务端维护的用户 ID 到精确权限 ID 数组的映射。Provider 根据 Principal.UserID 选择当前用户的权限；没有映射时仅查团队可见文档。search、fetch 和 document_status 使用相同规则。宿主必须限制连接配置的修改权限，不能让模型或普通浏览器调用者替换连接、Principal 或该映射。

文档读权限映射不授予写权限。`put_document` 从可信连接的 `document_permission_ids` 生成紧凑 ASCII JSON 请求头 `X-KB-Permission-Ids`，已有 SDK `RequestRef` 必须提供非空、稳定的逻辑命令 ID，并写入 `X-KB-Request-ID`。省略 ACL 不发该头；显式空数组发送 `[]`，代表团队可见。该配置最多 256 个去重 ID，每个不超过 128 UTF-8 字节，编码头最多 8192 字节；拒绝畸形输入。operation payload 与请求 Headers 不能覆盖此策略；DELETE 不发送 ACL 命令。宿主必须另外授权具体文档动作和固定目标库，并保证读写范围一致。产品还需保存文档归属、原文件、请求状态与代次，处理异步索引、结果不明、删除与迟到写入竞争。不能把接口受理当成这些治理已完成。

## bcri 与验证

依据接入时读取的控制台中 bcri 详情：`team_id=1470194374940573696`，`kb_id=kb-3bbd8f1d3249`。该实例标识只用于部署配置，不写入 Provider 实现。

```sh
go test ./providers/knowledge_base/http_api
go run ./scripts/verify_provider_release --provider knowledge_base/http_api --mode deterministic
```

确定性测试使用注入的 Transport fake，覆盖请求翻译、SDK/Catalog 身份、权限、内容保留及错误处理，包括 HTTP 200 业务失败。2026-09-10 已使用获准凭证完成成功空结果检索，不存在文档返回 HTTP 200 / `err_code: 1004`。随后从已登录的系统 Chrome 控制台确认推送 / 删除契约，专门创建的合成文档已推送并索引；真实 search 的 `/data/hits` 和 fetch 的 `/data/chunks` 均取得 4 个片段。首次 fetch 可能为 `PENDING` 且 chunks 为空，HTTP 成功不等于索引完成。非空文档字段映射由 Agent 配置并验证，Connector 继续保留完整成功 JSON。真实私有文档 ACL 尚未验收。此前 GET 管理详情探测返回 401，不能据此推断已验证的文档推送能力不可用。

同日新增文档生命周期接口已由 Agent 的实际 SDK / Provider / Transport 链路联调：一个初始确认不存在的合成文件成功推送，观察到 PENDING → CHUNKED → INDEXED，读取实际正文、搜索命中，再删除并检查不可见与该文档不再命中。联调耗时 30.36 秒，测试文档已清理；详细记录在 Agent 的 `docs/testing-2026-09-10-knowledge-document-protocol.md`。没有使用模型，也未上传私有用户资料；不作为私有 ACL 或产品文件管理验收。

Provider revision 1.2.0 增加私有上传连接策略、稳定请求头及已核对的上传限制，当时五个操作的输入 / 输出形状与契约摘要保持不变。2026-09-11 真实 Agent 产品已验证私有上传、三种阅读范围、重启、对话引用、关闭写入仍可读取，以及带回执的删除与 search 清理；见 Agent 的 `docs/testing-2026-09-11-private-library-upload.md`。索引同步任务、用户文件归属与后台管理页面由宿主应用继续实现。

当前 revision 1.3.0 将删除与上传的可靠性契约分开。依据 `kb-search-api` 提交 `dbf61406e55b8a446f026cde941ce7f9a9f33bda` 的 `handler/push.go:220` 和 `pkg/clients/lambda_client.go:72`：单文档删除同步执行 `purge_doc`，重复删除返回零统计，只有算子明确 `ok:true` 才成功；进行中、未清理完成或来源冲突仍返回失败。`delete_document` 契约摘要变为 `e4bec5d6d992035b91bf019b55a609307dc56f401c89ef78709f74f57e8e8aa8`，旧摘要在网络请求前拒绝，输入 / 输出形状不变。宿主只能在原物理来源、不可变文档 ID 和已提交删除意图下重试，并持久保存成功回执；ACL 范围内查不到文件不能单独证明删除。上传不因此取得重试能力。

# llm-proxy 票据 OCR 接入

## 调研依据

本次对接以 [llm-proxy PR #1073](https://github.com/codeck-backend/llm-proxy/pull/1073)
的提交 `7a942dc6ba3d497b05165ac27c3940f45659c408` 为准。PR 已合并，
是否已部署和启用仍需针对实际环境确认。

- [HTTP 协议与限制](https://github.com/codeck-backend/llm-proxy/blob/7a942dc6ba3d497b05165ac27c3940f45659c408/docs/google-expense-parser-api.md)：新增 `POST /llm/expense/parse`，接收 Base64 文档。
- [鉴权代码](https://github.com/codeck-backend/llm-proxy/blob/7a942dc6ba3d497b05165ac27c3940f45659c408/middleware/auth.go)：支持 Cookie、Bearer 和 `x-api-key`；`sk-` 前缀走 Passport API Key 验证，否则走 Token 验证。内部用户限制受环境判断控制。
- [服务实现](https://github.com/codeck-backend/llm-proxy/blob/7a942dc6ba3d497b05165ac27c3940f45659c408/service/expense/service.go)：调用 Google Document AI `:process`，读取 `text,entities`，Google 请求超时为 60 秒，响应上限 8 MiB。
- [响应模型](https://github.com/codeck-backend/llm-proxy/blob/7a942dc6ba3d497b05165ac27c3940f45659c408/model/expense/expense.go)：实体包含置信度、标准化金额/日期、从零开始的页码和递归明细。

这是票据/费用结构化识别。已有 `/llm/ocr` 是另一个接口，本次只接入新增的
Expense Parser。Google 项目、区域、Processor、版本和 Google 凭据均由
llm-proxy 管理，Domainry 不接受客户端覆盖这些资源标识。

## Domainry 实现

| 项目 | 值 |
| --- | --- |
| Connector | `expense_ocr` |
| Provider | `llm_proxy` |
| 操作 | `parse_expense`，同步调用 |
| 导入路径 | `github.com/domainry/domainry-connectors/providers/expense_ocr/llm_proxy` |
| 构造函数 | `New(connector.Transport)` |
| API | `POST {base_url}/llm/expense/parse` |
| 操作效果 | `write`，可能计费，无上游幂等或结果查询保证 |

Provider 使用 Runtime 注入的 HTTP Transport。凭据仅进入 `SecretHeaders`；
不创建独立 HTTP 客户端，不读取环境变量，不导入其他 Provider 或 Plane 内部包。
Catalog 已包含中英文产品定义、操作 SHA-256 和验证清单；只在项目选择该
Provider 时加入静态组合。

成功时去掉代理的 `code/msg/data` 外层，返回 `{text, entities}`。
`normalized_value` 使用 `json.RawMessage`，避免金额与大整数经过 `float64`
转换后丢失精度；递归 `properties` 和页码保留。

## 连接配置

在 Domainry Integration 中创建 `expense_ocr / llm_proxy` Connection：

```json
{
  "base_url": "https://YOUR-LLM-PROXY-HOST",
  "timeout_seconds": 90
}
```

`base_url` 只填写服务源地址，不带 `/llm/expense/parse`、查询参数、用户名或密码。
生产使用 HTTPS；本机测试可使用 `http://127.0.0.1:PORT`。
实际地址还必须符合 Runtime 出站策略，不能依赖 Provider 绕过网络限制。
`timeout_seconds` 范围是 1–300，默认 90，同时服从调用上下文更早的截止时间。

将 Secret 名称 `api_token` 绑定到已保存的原始 Passport API Key 或访问令牌。
不要把 `Bearer ` 前缀存入 Secret；Provider 会自动添加它。
`sk-…` API Key 和普通 Token 都通过 `Authorization: Bearer …` 发送。

上游需要已启用 `[app.expense_parser]`，并配置有效的 Google Processor 与凭据。
PR 中所有环境默认关闭此功能。认证成功不代表 Google 识别已经可用。

没有独立的非计费识别就绪接口，因此本 Provider 不提供 `ConnectionTester`。
应通过显式执行一次 `parse_expense` 验证真实能力；Secret 的测试策略使用 SDK
已有的 `optional`。Catalog 校验已补齐该策略，仍拒绝未知策略值。

## 输入与调用

```json
{
  "document": "<标准 Base64 文档内容或 MIME 一致的 data URL>",
  "mime_type": "image/jpeg",
  "session_id": "session-1",
  "conv_id": "conversation-1",
  "react_id": "reaction-1"
}
```

支持 `application/pdf`、`image/tiff`、`image/jpeg`、`image/png`、`image/bmp`、
`image/gif` 和 `image/webp`。Provider 在调用上游前检查 Base64、文件签名和
MIME 一致性；解码后的文档不得超过 20 MiB。请求 JSON 不超过 29 MiB，
不启用 gzip。关联标识可省略；每个最多 256 字节，不能含换行或 NUL，
它们只是日志关联字段，不是幂等键。

业务代码通过项目已有的 Gateway 调用：

```go
import (
    "context"
    connector "github.com/domainry/domainry-connector-sdk"
    llmproxy "github.com/domainry/domainry-connectors/providers/expense_ocr/llm_proxy"
)

func ParseReceipt(ctx context.Context, gateway connector.Gateway, documentBase64 string) (llmproxy.ParseExpenseOutput, error) {
    return connector.Call(ctx, gateway, llmproxy.ParseExpense, llmproxy.ParseExpenseInput{
        Document: documentBase64,
        MIMEType: "image/jpeg",
    })
}
```

上面的 20 MiB 是 Provider/上游能力。当前 Integration SaaS 通用 HTTP 调用
入口在 `internal/transport/http/saas/handler.go` 中限制 JSON 为 1 MiB，
不能把大文件直接放进该入口。较大文件应由业务后端读取受控上传后在进程内
调用 Gateway；具体业务上传接口需要独立验收，本次没有增加文件上传 API。
票据正文也不应被误认为错误日志：Integration 默认调用记录会保存请求与响应；
业务若要求仅保存摘要，应使用已有的敏感调用持久化模式，并自行保存获准保留的结果。

## 失败与重试语义

| 上游结果 | Domainry 分类 |
| --- | --- |
| 本地文件、配置、凭据校验失败 | `permanent`，不发出请求 |
| 400 / 413 / 415 / 422 | `permanent`，文档被拒绝 |
| 401 / 403 | `permanent`，认证或环境权限问题 |
| 404 / 405 | `permanent`，接口不可用 |
| 429 | `retryable`，仍须由 Runtime 策略决定是否重试 |
| 503 | `permanent`，按该 PR 的语义表示功能配置或 Google 凭据不可用 |
| 网络失败、502、504、其他不明确状态 | `uncertain`，请求可能已处理 |
| HTTP 200 但返回格式不完整或非法 | `uncertain`，不能确认识别结果 |

Provider 不自行重试，不把任意上游错误正文、传输错误原文、凭据或文档附到
错误链中。HTTP 状态只作为 `http:<status>` 响应引用保留。
SDK 当前没有 `Retry-After` 传递字段，因此不声称执行了上游建议的退避时间。
HTTP 200 的业务 `code` 必须显式存在且为 0；缺失 `data/text/entities` 不会
被当成空识别成功。有效的空文本与空实体数组可以正常返回。

## Runtime 配套与发布

当前 Runtime 使用 Foundation `safehttp.NewClient`，默认客户端超时是 30 秒。
只在 Provider 中设置 90 秒不能突破该客户端限制。本次同时修改
`domainry-runtime/pkg/runtimehost/connector_transport.go`：若调用上下文已有
截止时间，则使用该截止时间约束请求；没有截止时间时继续使用客户端默认超时。
通过复制客户端处理单次请求，不修改共享客户端或出站检查规则。

当前 Plane 的 `internal/controlplane/authoring/definition/connector_summary_catalog.go`
和 `connector_detail.go` 已从 Connectors 读取产品定义，
`internal/controlplane/connectorresolution/official_catalog.go` 按 Catalog 选择
Provider，所以不需要在 Plane 增加专用 OCR 路由或注册分支。

本次是本地源码接入，没有发布 tag 或部署。正式启用须发布包含该 Provider 的
Connectors 版本和包含超时修复的 Runtime 版本，再更新 Plane/目标项目的依赖，
重新生成 Catalog 解析、锁文件与选中 Provider 的静态组合。
SDK 公共契约没有改动。不得把临时本地依赖映射作为发布输入。

## 验证结果

已完成 Provider 与 Catalog 契约测试、七种文件格式及 20 MiB 边界测试、金额/
日期/大整数保真、错误分类、无凭据泄漏、调用截止时间和隔离 HTTP 协议验证。
已通过相关包的 race 检查、`go vet`、格式/依赖边界和 Catalog/Registry 生成一致性检查。
Runtime 的默认超时、显式截止时间和已过期请求回归测试通过。

使用临时 Go 依赖映射验证了当前 Plane 能读取 `expense_ocr` 定义并解析出唯一
`llm_proxy / parse_expense` 选择；该映射仅用于本地验证。

尚未进行真实 llm-proxy/Google 识别验收：实际服务地址、可用的 Domainry
Connection/Secret 和获准用于识别的票据样本尚未提供。

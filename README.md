# prefect-dingtalk-notification

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/Prefect-3-0B3B82?logo=prefect&logoColor=white)](https://docs.prefect.io/)

一个用于 Prefect 3 的钉钉自定义机器人通知 Block。它通过钉钉群机器人的 Webhook 发送文本或 Markdown 等完整消息 JSON，支持加签，并可以直接用于 Prefect Automation 的 `SendNotification` action。

## 特性

- 实现 Prefect `NotificationBlock`，可保存到 Prefect Server 并在 Automation 中复用。
- 使用钉钉自定义机器人 Webhook，支持 `access_token` 和可选的 HMAC-SHA256 加签密钥。
- 异步发送 HTTP 请求，检查 HTTP 状态、响应 JSON 和钉钉 `errcode`。
- 将网络错误、无效响应和钉钉业务错误统一转换为 Prefect `NotificationError`。
- 提供 Docker 驱动的真实集成测试，自动验证直接调用和 Automation 调用。

## 兼容性

- Python `>=3.10,<3.15`
- Prefect `>=3.8,<4`
- httpx `>=0.27,<1`
- pydantic `>=2,<3`
- 钉钉自定义机器人 Webhook

测试环境使用官方 `prefecthq/prefect:3-latest` 镜像。不要使用无版本前缀的 `prefecthq/prefect:latest`：该标签当前是遗留的 Prefect 1 镜像，无法运行本项目的 Prefect 3 API。

## 安装

本项目使用 [uv](https://docs.astral.sh/uv/) 管理依赖。发布到 PyPI 后，可直接安装：

```bash
uv add prefect-dingtalk-notification
```

或获取源码进行开发：

```bash
git clone https://github.com/xcRRRRt/prefect-dingtalk-notification.git
cd prefect-dingtalk-notification
uv sync
```

安装后，Prefect 会通过项目声明的 `prefect.collections` entry point 发现这个 Block：

```python
from prefect_dingtalk_notification import (
    DingTalkCustomRobotGroupWebhookNotification,
)
```

## 直接发送通知

`access_token` 是必填项。`secret` 在机器人启用加签时填写；如果机器人没有启用加签，可以省略。`body` 应该是钉钉接受的完整 JSON 字符串，`subject` 由 Prefect 接口传入，但当前钉钉 Webhook 不使用它。

```python
import asyncio
import json
import os

from prefect_dingtalk_notification import (
    DingTalkCustomRobotGroupWebhookNotification,
)


async def main() -> None:
    notification = DingTalkCustomRobotGroupWebhookNotification(
        access_token=os.environ["DINGTALK_ACCESS_TOKEN"],
        secret=os.environ.get("DINGTALK_SECRET"),
    )
    await notification.notify(
        body=json.dumps(
            {
                "msgtype": "text",
                "text": {"content": "Prefect notification test"},
            },
            ensure_ascii=False,
        ),
        subject="Prefect notification",
    )


asyncio.run(main())
```

默认 Webhook 地址是 `https://oapi.dingtalk.com/robot/send`。如果需要测试代理或兼容的网关，可以传入自定义 `base_url`。

## 保存为 Prefect Block

Block 保存后，Prefect Server 会保存配置并在需要时重新加载它。敏感字段使用 Pydantic `SecretStr` 定义，不要把 token 或 secret 写入代码仓库。

```python
from prefect_dingtalk_notification import (
    DingTalkCustomRobotGroupWebhookNotification,
)

notification = DingTalkCustomRobotGroupWebhookNotification(
    access_token="<access-token>",
    secret="<secret>",
)
block_document_id = notification.save("dingtalk-production")
print(block_document_id)
```

在其他 Flow 或脚本中可以按名称加载：

```python
notification = DingTalkCustomRobotGroupWebhookNotification.load(
    "dingtalk-production"
)
```

## 在 Automation 中发送通知

Prefect Automation 的 `SendNotification` action 接受 Block document ID。下面的示例会在收到指定事件时调用本项目的 Block：

```python
import os

from prefect.automations import Automation, EventTrigger, SendNotification
from prefect.events import emit_event
from prefect_dingtalk_notification import (
    DingTalkCustomRobotGroupWebhookNotification,
)

notification = DingTalkCustomRobotGroupWebhookNotification(
    access_token=os.environ["DINGTALK_ACCESS_TOKEN"],
    secret=os.environ.get("DINGTALK_SECRET"),
)
block_id = notification.save("dingtalk-ci")
event_name = "example.build.finished"

automation = Automation(
    name="notify-dingtalk-on-build",
    trigger=EventTrigger(
        expect={event_name},
        match={"environment": "ci"},
    ),
    actions=[
        SendNotification(
            block_document_id=block_id,
            subject="Build notification",
            body=(
                '{"msgtype":"text","text":{"content":"Prefect CI build finished"}}'
            ),
        )
    ],
).create()

emit_event(
    event=event_name,
    resource={
        "prefect.resource.id": "example.build.123",
        "environment": "ci",
    },
)
```

Automation 的后台服务必须安装本项目，才能根据 Block document 反序列化并执行通知。测试 Docker 镜像已经包含这一步。

## 配置和安全

真实测试可以从环境变量或项目根目录的 `.env` 读取配置：

```dotenv
DINGTALK_ACCESS_TOKEN=
DINGTALK_SECRET=
DINGTALK_KEYWORD=Prefect
```

`.env` 已被 Git 忽略；可以从 [.env.example](.env.example) 开始。`DINGTALK_KEYWORD` 只在机器人启用了自定义关键词安全时需要，测试消息会把它包含在文本内容中。

不要提交 Webhook token、加签密钥或包含它们的 Prefect 配置导出。CI 应使用仓库 Secrets 注入 `DINGTALK_ACCESS_TOKEN` 和 `DINGTALK_SECRET`。

## 测试

普通测试不会启动 Docker，也不会向钉钉发送消息：

```bash
uv run pytest
```

真实集成测试需要显式加上项目自定义的 `--run-integration` 参数：

```bash
uv run pytest --run-integration
```

这个参数不是 pytest 内置选项，而是本项目的安全开关。启用后测试会：

1. 拉取 `prefecthq/prefect:3-latest`，构建并启动独立的 Prefect Server。
2. 等待 `http://127.0.0.1:14200/api/health` 就绪。
3. 执行一次直接 Notification 调用。
4. 创建 Block 和 Automation，发送专用事件，并等待 `prefect.automation.action.executed`。
5. 在成功或失败后删除测试资源和 Docker 容器。

直接调用通过钉钉接口的 `errcode == 0` 判断。Automation 调用通过 Prefect 的 `action.executed` 判断；`action.failed` 或 120 秒超时都会使测试失败。测试不依赖读取群消息，也不要求人工检查。

## GitHub Actions

仓库包含一个手动触发的 [DingTalk integration workflow](.github/workflows/dingtalk-integration.yml)。配置以下仓库 Secrets 后，在 Actions 页面手动运行即可；`DINGTALK_ACCESS_TOKEN` 必填，`DINGTALK_SECRET` 仅在机器人启用加签时需要：

- `DINGTALK_ACCESS_TOKEN`
- `DINGTALK_SECRET`

成功运行会发送两条真实消息，因此工作流不会在每次 push 时自动触发。可选的仓库变量 `DINGTALK_KEYWORD` 用于覆盖默认关键词 `Prefect`。

## 发布到 PyPI

`.github/workflows/publish.yml` 在 GitHub Release 发布后自动构建并发布 Python wheel 和 source distribution。发布前会执行锁文件检查、Ruff、普通测试、版本匹配检查和 `twine check`；集成测试需要真实钉钉凭据，因此不会作为发布门禁自动发送消息。

工作流使用 PyPI Trusted Publishing（OIDC），不需要在仓库中保存 PyPI API token。首次使用前完成以下配置：

1. 如果项目已存在，在 PyPI 项目设置的 Publishing 页面添加 GitHub Actions Trusted Publisher；首次发布新项目时，则在账号级 Publishing 页面创建 pending publisher。两种情况都填写 owner `xcRRRRt`、repository `prefect-dingtalk-notification`、workflow `publish.yml`、environment `pypi`，新项目还要填写 PyPI 项目名 `prefect-dingtalk-notification`。
2. 在 GitHub 仓库创建名为 `pypi` 的 Environment；可以按需要启用人工审核保护规则。
3. 修改 `pyproject.toml` 中的 `version` 后运行 `uv lock`，提交这两个文件并创建同版本 GitHub Release。Release tag 可以使用 `v0.1.0`，工作流会自动去掉前缀 `v`，并拒绝与项目版本不一致的发布。

PyPI Trusted Publishing 的配置说明见 [PyPI 文档](https://docs.pypi.org/trusted-publishers/)；发布动作使用 [pypa/gh-action-pypi-publish](https://github.com/pypa/gh-action-pypi-publish)。

## 发布到 TestPyPI

`.github/workflows/publish-testpypi.yml` 是手动触发的预发布流程。它允许在 Actions 页面选择 Git ref，执行与正式发布相同的检查和打包步骤，然后将构建产物上传到 TestPyPI，并从 TestPyPI 创建隔离环境验证安装、导入和 Prefect collection entry point。每个版本在 TestPyPI 上也不可覆盖；重复测试同一版本前请先更新 `version` 并运行 `uv lock`。

首次使用前，在 TestPyPI 账号的 Publishing 页面添加 GitHub Actions Trusted Publisher：owner `xcRRRRt`、repository `prefect-dingtalk-notification`、workflow `publish-testpypi.yml`、environment `testpypi`。同时在 GitHub 仓库创建名为 `testpypi` 的 Environment。工作流使用 OIDC，不需要 TestPyPI API token。

运行路径：打开 GitHub Actions，选择 **Publish package to TestPyPI**，点击 **Run workflow**，填写要测试的 branch、tag 或 commit ref。TestPyPI 支持使用与 PyPI 相同的 Trusted Publishing 配置方式；发布动作通过 `repository-url: https://test.pypi.org/legacy/` 指向 TestPyPI。发布成功后，smoke test 会等待索引可见并安装刚发布的精确版本。

仓库还包含在 push 和 pull request 上运行的 [CI workflow](.github/workflows/ci.yml)，覆盖 Python 3.10–3.14，并检查锁文件、代码风格、类型、普通测试和发行包元数据。

## 开发

```bash
uv run ruff check tests src
uv run pytest
uv lock --check
```

项目目录结构：

```text
src/prefect_dingtalk_notification/
├── __init__.py
├── notification.py
└── py.typed
tests/
├── conftest.py
└── integration/
    └── test_dingtalk_notification.py
Dockerfile.test
docker-compose.test.yml
```

## 错误处理

通知请求出现以下情况时会抛出 Prefect `NotificationError`：

- HTTP 请求失败或响应状态码不是成功状态。
- 响应不是包含 `errcode` 和 `errmsg` 的有效 JSON。
- 钉钉返回非零 `errcode`，例如 token、签名或关键词校验失败。

钉钉自定义机器人接口只提供发送结果，不提供通过该 Webhook 回读群消息的能力。因此 CI 验收使用接口返回值和 Prefect Automation 事件，不把“群内人工可见”作为自动化通过条件。

## 许可证

本项目使用 [MIT License](LICENSE)。

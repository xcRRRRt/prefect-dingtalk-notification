# prefect-dingtalk-notification

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/Prefect-3-0B3B82?logo=prefect&logoColor=white)](https://docs.prefect.io/)

一个用于 [Prefect](https://www.prefect.io/) 的钉钉自定义机器人通知 Block

## 安装

```bash
uv add prefect-dingtalk-notification
```

## 使用

### 添加自定义机器人

添加[自定义机器人](https://open.dingtalk.com/document/development/custom-robots-send-group-messages)

注: 选择 **自定义 (通过Webhook接入自定义服务)** 机器人

### 使用 `DingTalkCustomRobotGroupWebhookNotification`

#### 直接发送通知

```python
import asyncio
import json

from prefect_dingtalk_notification import DingTalkCustomRobotGroupWebhookNotification


async def main() -> None:
    notification = DingTalkCustomRobotGroupWebhookNotification(
        access_token="<access-token>",
        secret="<secret>",  # 可选, 基于你的机器人设置
    )
    await notification.notify(
        body=json.dumps(
            {
                "msgtype": "text",
                "text": {"content": "Prefect notification test"},
            },
            ensure_ascii=False,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
```

> [!NOTE]
> `body` 是钉钉机器人消息的完整 JSON 请求体，会作为 HTTP 请求体原样发送，需要按
> [消息类型](https://open.dingtalk.com/document/development/custom-robots-send-group-messages)
> 自行构建字符串

> [!NOTE]
> `DingTalkCustomRobotGroupWebhookNotification.notify` 的 `subject` 参数无效, 若传入会被忽略

#### 保存为 `Block` 使用

```python
from prefect_dingtalk_notification import DingTalkCustomRobotGroupWebhookNotification

notification_block = DingTalkCustomRobotGroupWebhookNotification(
    access_token="<access-token>",
    secret="<secret>",
)
block_document_id = notification_block.save("dingtalk-demo")
```

加载 `Block`

```python
from prefect_dingtalk_notification import DingTalkCustomRobotGroupWebhookNotification

notification_block = DingTalkCustomRobotGroupWebhookNotification.load(
    "dingtalk-production"
)
```

#### 在 Automation 中发送通知

Prefect Automation 的 `SendNotification` action 接受 Block document ID。下面的示例会在收到指定事件时调用本项目的 Block：

```python
from prefect.automations import Automation, EventTrigger, SendNotification
from prefect.events import emit_event
from prefect_dingtalk_notification import DingTalkCustomRobotGroupWebhookNotification

notification = DingTalkCustomRobotGroupWebhookNotification(
    access_token="<access-token>",
    secret="<secret>",
)
block_id = notification.save("dingtalk-demo")
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
            subject="",  # leave blank
            body='{"msgtype":"text","text":{"content":"Prefect notification test"}}',
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

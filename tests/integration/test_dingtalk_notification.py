from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from prefect.automations import Automation, EventTrigger, SendNotification
from prefect.client.orchestration import get_client
from prefect.events import emit_event
from prefect.events.filters import EventFilter, EventOccurredFilter

from prefect_dingtalk_notification import (
    DingTalkCustomRobotGroupWebhookNotification,
)

pytestmark = pytest.mark.integration


def _message(kind: str, run_id: str, keyword: str) -> str:
    return json.dumps(
        {
            "msgtype": "text",
            "text": {"content": f"{keyword} Prefect DingTalk CI test {kind} {run_id}"},
        },
        ensure_ascii=False,
    )


@pytest.mark.asyncio
async def test_direct_notification(
    prefect_server: str, dingtalk_credentials: dict[str, str]
) -> None:
    del prefect_server
    run_id = uuid4().hex
    block = DingTalkCustomRobotGroupWebhookNotification(
        access_token=dingtalk_credentials["access_token"],
        secret=dingtalk_credentials["secret"],
    )

    await block.notify(
        body=_message("direct", run_id, dingtalk_credentials["keyword"]),
        subject="Prefect DingTalk direct integration test",
    )


@pytest.mark.asyncio
async def test_automation_sends_notification(
    prefect_server: str, dingtalk_credentials: dict[str, str]
) -> None:
    del prefect_server
    run_id = uuid4().hex
    block_name = f"ci-dingtalk-{run_id}"
    event_name = "prefect-dingtalk-notification.test"
    resource_id = f"prefect-dingtalk-notification.test.{run_id}"
    block = DingTalkCustomRobotGroupWebhookNotification(
        access_token=dingtalk_credentials["access_token"],
        secret=dingtalk_credentials["secret"],
    )
    block_id = await block.asave(name=block_name)
    automation: Automation | None = None
    started_at = datetime.now(timezone.utc)

    try:
        automation = await Automation(
            name=f"ci-dingtalk-{run_id}",
            trigger=EventTrigger(
                expect={event_name},
                match={"ci_test_id": run_id},
                threshold=1,
                within=timedelta(seconds=0),
            ),
            actions=[
                SendNotification(
                    block_document_id=block_id,
                    subject="Prefect DingTalk Automation integration test",
                    body=_message(
                        "automation", run_id, dingtalk_credentials["keyword"]
                    ),
                )
            ],
        ).acreate()

        emitted = emit_event(
            event=event_name,
            resource={"prefect.resource.id": resource_id, "ci_test_id": run_id},
        )
        assert emitted is not None, "the test event was not queued for Prefect"
        triggering_event_resource_id = f"prefect.event.{emitted.id}"
        automation_resource_id = f"prefect.automation.{automation.id}"

        deadline = time.monotonic() + 120
        observed: str | None = None
        while time.monotonic() < deadline:
            async with get_client() as client:
                page = await client.read_events(
                    filter=EventFilter(
                        occurred=EventOccurredFilter(
                            since=started_at,
                        )
                    ),
                    limit=50,
                )
                for event in page.events:
                    involved_ids = {event.resource.id} | {
                        related.id for related in event.related
                    }
                    if (
                        triggering_event_resource_id not in involved_ids
                        or automation_resource_id not in involved_ids
                    ):
                        continue
                    if event.event == "prefect.automation.action.executed":
                        observed = event.event
                        break
                    if event.event == "prefect.automation.action.failed":
                        raise AssertionError(
                            "Prefect Automation action failed for the test event; "
                            f"automation={automation.id}, event={emitted.id}"
                        )
            if observed:
                break
            await asyncio.sleep(2)

        assert observed == "prefect.automation.action.executed", (
            "timed out waiting for prefect.automation.action.executed; "
            "inspect the Prefect server logs for the action failure"
        )
    finally:
        if automation is not None:
            await automation.adelete()
        await DingTalkCustomRobotGroupWebhookNotification.adelete(name=block_name)

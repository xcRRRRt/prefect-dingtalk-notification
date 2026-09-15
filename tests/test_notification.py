from __future__ import annotations

import traceback

import httpx
import pytest
import respx
from prefect.blocks.abstract import NotificationError

from prefect_dingtalk_notification import DingTalkCustomRobotGroupWebhookNotification
from prefect_dingtalk_notification import notification as notification_module

BASE_URL = "https://dingtalk.example.test/robot/send"
ACCESS_TOKEN = "access-token-for-tests"
SECRET = "test-secret"
FIXED_TIMESTAMP = "1700000000123"
EXPECTED_SIGNATURE = "emvuTAxMTWOXmz4z3p3ifyzxqMNeLEBESaq9BQR0d6w="


def _notification(
    *,
    access_token: str = ACCESS_TOKEN,
    secret: str | None = None,
) -> DingTalkCustomRobotGroupWebhookNotification:
    return DingTalkCustomRobotGroupWebhookNotification(
        base_url=BASE_URL,
        access_token=access_token,
        secret=secret,
    )


def _assert_redacted(
    caplog: pytest.LogCaptureFixture,
    error: pytest.ExceptionInfo[NotificationError],
    sensitive_values: tuple[str, ...],
) -> None:
    formatted_traceback = "".join(
        traceback.format_exception(error.type, error.value, error.tb)
    )
    surfaces = (caplog.text, error.value.log, formatted_traceback)
    for value in sensitive_values:
        assert all(value not in surface for surface in surfaces)


@pytest.mark.asyncio
async def test_notify_posts_unsigned_json_to_configured_webhook() -> None:
    body = '{"msgtype":"markdown","markdown":{"title":"构建完成","text":"# 构建完成\\n部署成功"}}'

    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post(BASE_URL).respond(200, json={"errcode": 0, "errmsg": "ok"})

        await _notification().notify(body=body, subject="this subject is ignored")

    request = route.calls.last.request
    assert route.call_count == 1
    assert str(request.url).split("?", maxsplit=1)[0] == BASE_URL
    assert dict(request.url.params) == {"access_token": ACCESS_TOKEN}
    assert request.headers["content-type"] == "application/json"
    assert request.content == body.encode("utf-8")


@pytest.mark.asyncio
async def test_notify_adds_expected_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        notification_module.time,
        "time",
        lambda: int(FIXED_TIMESTAMP) / 1000,
    )

    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post(BASE_URL).respond(200, json={"errcode": 0, "errmsg": "ok"})

        await _notification(secret=SECRET).notify(body='{"msgtype":"text"}')

    request = route.calls.last.request
    assert route.call_count == 1
    assert dict(request.url.params) == {
        "access_token": ACCESS_TOKEN,
        "timestamp": FIXED_TIMESTAMP,
        "sign": EXPECTED_SIGNATURE,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 401, 429, 500])
async def test_notify_converts_http_errors_without_leaking_credentials(
    status_code: int,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        notification_module.time,
        "time",
        lambda: int(FIXED_TIMESTAMP) / 1000,
    )

    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post(BASE_URL).respond(status_code, text="request rejected")

        with pytest.raises(NotificationError) as captured:
            await _notification(secret=SECRET).notify(body='{"msgtype":"text"}')

    assert route.call_count == 1
    assert captured.value.log == (
        "DingTalkCustomRobotGroupWebhookNotification notify request failed "
        f"(HTTP status {status_code})"
    )
    _assert_redacted(
        caplog,
        captured,
        (ACCESS_TOKEN, SECRET, EXPECTED_SIGNATURE),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_error", "detail"),
    [
        pytest.param(httpx.ConnectError("connection failed"), "ConnectError"),
        pytest.param(httpx.ReadTimeout("read timed out"), "ReadTimeout"),
    ],
    ids=["connection-error", "read-timeout"],
)
async def test_notify_converts_network_errors_without_leaking_credentials(
    request_error: httpx.HTTPError,
    detail: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        notification_module.time,
        "time",
        lambda: int(FIXED_TIMESTAMP) / 1000,
    )

    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        route = router.post(BASE_URL).mock(side_effect=request_error)

        with pytest.raises(NotificationError) as captured:
            await _notification(secret=SECRET).notify(body='{"msgtype":"text"}')

    assert route.call_count == 1
    assert captured.value.log == (
        "DingTalkCustomRobotGroupWebhookNotification notify request failed "
        f"({detail})"
    )
    _assert_redacted(
        caplog,
        captured,
        (ACCESS_TOKEN, SECRET, EXPECTED_SIGNATURE),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_content",
    [
        pytest.param(b"not JSON", id="not-json"),
        pytest.param(b'{"errmsg":"ok"}', id="missing-errcode"),
        pytest.param(b'{"errcode":0}', id="missing-errmsg"),
        pytest.param(b'{"errcode":"not-an-integer","errmsg":"ok"}', id="invalid-errcode"),
    ],
)
async def test_notify_rejects_malformed_dingtalk_responses(
    response_content: bytes,
) -> None:
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.post(BASE_URL).respond(200, content=response_content)

        with pytest.raises(NotificationError) as captured:
            await _notification().notify(body='{"msgtype":"text"}')

    assert "Unexpected DingTalkCustomRobotGroupWebhookNotification notify response" in (
        captured.value.log
    )


@pytest.mark.asyncio
async def test_notify_rejects_dingtalk_business_errors() -> None:
    with respx.mock(assert_all_called=True, assert_all_mocked=True) as router:
        router.post(BASE_URL).respond(
            200,
            json={"errcode": 310000, "errmsg": "invalid access token"},
        )

        with pytest.raises(NotificationError) as captured:
            await _notification().notify(body='{"msgtype":"text"}')

    assert "310000" in captured.value.log
    assert "invalid access token" in captured.value.log

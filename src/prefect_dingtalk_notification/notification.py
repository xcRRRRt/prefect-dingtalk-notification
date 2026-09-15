"""
DingTalk Custom Robot Group Messages

https://open.dingtalk.com/document/development/custom-robots-send-group-messages
"""
import base64
import hashlib
import hmac
import time

import httpx
from prefect.blocks.abstract import NotificationBlock, NotificationError
from pydantic import BaseModel, Field, SecretStr, ValidationError


class DingTalkCustomRobotGroupWebhookResponse(BaseModel):
    code: int = Field(alias="errcode")
    message: str = Field(alias="errmsg")


class DingTalkCustomRobotGroupWebhookNotification(NotificationBlock):
    _block_type_name = "DingTalk Robot Notification"
    _logo_url = "https://img.alicdn.com/imgextra/i3/O1CN017PqYP51OX3bSJGxQY_!!6000000001714-2-tps-200-200.png"
    _description = "DingTalk Custom Robot Group Message Using Webhook"

    base_url: str = Field(default="https://oapi.dingtalk.com/robot/send")
    access_token: SecretStr = Field()
    secret: SecretStr | None = Field(default=None)

    async def notify(self, body: str, subject: str | None = None) -> None:
        """
        Send Notification

        Args:
            body: Message body
            subject: Not used, will be ignored
        """
        access_token = self.access_token.get_secret_value()
        params = {"access_token": access_token}

        if self.secret is not None:
            secret = self.secret.get_secret_value()
            timestamp = str(round(time.time() * 1000))
            string_to_sign = f"{timestamp}\n{secret}"
            hmac_code = hmac.new(
                secret.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            params["timestamp"] = timestamp
            params["sign"] = base64.b64encode(hmac_code).decode("utf-8")

        logger = self.logger

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(
                    self.base_url,
                    params=params,
                    content=body,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
            except httpx.HTTPError as e:
                if isinstance(e, httpx.HTTPStatusError):
                    detail = f"HTTP status {e.response.status_code}"
                else:
                    detail = e.__class__.__name__
                error_log = f"{self.__class__.__name__} notify request failed ({detail})"
                # The request URL contains access_token and sign query
                # parameters. Do not log or propagate the raw HTTPX error.
                logger.error(error_log)
                raise NotificationError(log=error_log) from None

            try:
                result = DingTalkCustomRobotGroupWebhookResponse.model_validate_json(
                    resp.content,
                    by_alias=True,
                )
            except ValidationError as e:
                logger.exception(
                    f"Unexpected {self.__class__.__name__} notify response content: {resp.content}",
                    exc_info=e,
                )
                raise NotificationError(
                    log=f"Unexpected {self.__class__.__name__} notify response content: {resp.content}; {e}"
                ) from e

            logger.info(str(result))
            if result.code != 0:
                raise NotificationError(
                    log=f"Unexpected {self.__class__.__name__} notify error code: {result!s}"
                )

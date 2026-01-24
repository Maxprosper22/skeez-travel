from itsdangerous.url_safe import URLSafeTimedSerializer
from datetime import datetime, timezone
from enum import Enum
from sanic import Sanic

from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from sanic import Sanic
import smtplib
import pprint

class MagicEnum(Enum):
    VERICATION = "verification"
    RESET = "reset"


class SecureMagicLink:
    def __init__(self, app: Sanic, secret_key: str, link_type: str):
        self.app = app
        self.serializer = URLSafeTimedSerializer(secret_key, salt=link_type)
        self.link_type = link_type


    async def create_link(self, accountid: str, email: str, expires_min: int = 10) -> str:
        payload = {
            "account_id": accountid.hex,
            "email": email,
            "iat": int(datetime.now(timezone.utc).timestamp())
        }
        token = self.serializer.dumps(payload)

        match self.app.config.app['ENV']:
            case 'dev':
                match self.link_type:
                    case "verify":
                        return f"https://{self.app.config.app['HOST']}:{self.app.config.app['PORT']}/account/verify?token={token}"
                    case "reset":
                        return f"https://{self.app.config.app['HOST']}:{self.app.config.app['PORT']}/account/reset/link?token={token}"

            case 'prod':
                match self.link_type:
                    case "verify":
                        return f"https://{self.app.config.app['DOMAIN']}/account/verify?token={token}"
                    case "reset":
                        return f"https://{self.app.config.app['DOMAIN']}/accoint/reset?token={token}"


    async def verify_token(self, token: str, max_age: int = 600) -> dict | None:
        try:
            return self.serializer.loads(token, max_age=max_age)
        except Exception as e:
            pprint.pp(e)
            # return None
            raise e




def email_verification(password: str, link: str, messageFrom: str, messageTo: str):
    """ Function for sending email verification links """
    try:
        message = EmailMessage()
        message['FROM'] = messageFrom
        message['TO'] = messageTo
        message['SUBJECT'] = "Account Verification"
        message.set_content(f"{link}")

        with smtplib.SMTP("smtp.zoho.com", 587) as server:
            print(server.__dir__())
            server.starttls()
            server.login(messageFrom, password)
            server.send_message(message)

    except Exception as e:
        raise e


def password_reset(password: str, link: str, messageFrom: str, messageTo: str):
    """ Function for Sending password reset links """
    try:
        message = EmailMessage()
        message['FROM'] = messageFrom
        message['TO'] = messageTo
        message['SUBJECT'] = "Password Reset"
        message.set_content(f"{link}")

        with smtplib.SMTP("smtp.zoho.com", 587) as server:
            server.starttls()
            server.login(messageFrom, password)
            server.send_message(message)

    except Exception as e:
        raise e


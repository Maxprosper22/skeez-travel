import os
from pydantic import BaseModel, Field
from typing import Optional

from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import aiosmtplib

from sanic import Sanic
from src.models.account import Account

class MailConfig(BaseModel):
    host: str
    port: int
    username: str
    password: str


def load_mail_config():
    """ Load email config. Dependent on tye environment """
    mail_config = {}
    mail_config['SUPPORT'] = os.getenv("MAIL_SUPPORT")
    mail_config['ADMIN'] = os.getenv("MAIL_ADMIN")
    mail_config["NOREPLY"] = os.getenv("MAIL_NOREPLY")
    mail_config["PASSWORD"] = os.getenv("MAIL_PASSWORD")  # Get password from env
            
    if not mail_config["PASSWORD"]:
        raise ValueError("DEV_SMTP_PASSWORD environment variable is not set")
            
    return mail_config

        
async def welcome_mail(app: Sanic, config: MailConfig, message: EmailMessage, account: Account):
    """ Welcome enail sent to new users """
    
    message = MIMEMultipart("alternative")
    message["From"] = "root@localhost"
    message["To"] = "somebody@example.com"
    message["Subject"] = "Welcome Message"

    message.attach(MIMEText(
        """<html>
            <body>
                <h1>Welcome! Your account was created successfully.</h1>
            </body>
        </html>
        """,
        "html",
        "utf-8"
        ))

    await aiosmtplib.send(
        message,
        hostname=config.host,
        port=config.port,
        username=config.username,
        password=config.password
    )


async def confirmation_mail(app: Sanic, config: MailConfig, message: EmailMessage, account: Account):
    """ Email sent to confirm  users' email account """

async def password_reset_mail(app: Sanic, config: MailConfig, message: EmailMessage, account: Account):
    """ Email containing a link and token sent when a password change is requested """

async def trip_alert(app: Sanic, config: MailConfig, message: EmailMessage, account: Account):
    """ Email sent to alert a user about appraoching date of trips """
    try:
        pass
    except Exception as e:
        raise e

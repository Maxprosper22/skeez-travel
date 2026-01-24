from dotenv import load_dotenv
from sanic import Sanic
from sanic.response import html as sanhtml, json as sanjson, file as sanfile
from sanic.log import logger
from sanic_ext import Extend
# from sanic_cors import CORS, cross_origin
import toml
import os
from pathlib import Path
from asyncio import create_subprocess_shell
from asyncio.subprocess import PIPE

import httpx, aiohttp

from src.utils.db import db_conn, db_pool
from src.utils.tasks import trip_task, transaction_pooler

from src.services import register_services
from src.services.celery_app import app as celeryapp, email_verification, email_reset_link
from src.services.mail import load_mail_config

from src.services.magiclink import SecureMagicLink


from src.blueprints import register_apps

from cryptography.fernet import Fernet

from src.middlewares import setup_middleware
from src.urls import router

from apscheduler.schedulers.asyncio import AsyncIOScheduler
# from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

# from sqlalchemy.ext.asyncio import create_async_engine

import pprint


load_dotenv()


def load_database_config():
    """ Load database config. Dependent on the environment """
    try:
        db_config = {}
        db_config["DB_NAME"] = os.getenv("PGDATABASE")
        db_config["DB_USER"] = os.getenv("PGUSER")
        db_config["DB_HOST"] = os.getenv("PGHOST")
        db_config["DB_PORT"] = os.getenv("PGPORT")
        db_config["DB_PASSWORD"] = os.getenv("PGPASSWORD")  # Get password from env

        if not db_config["DB_PASSWORD"]:
            raise ValueError("DB_PASSWORD environment variable is not set")

        return db_config
    except Exception as e:
        logger.error(e)
        # raise e


def load_paystack_config():
    """ Load paystack configuration """
    try:
        paystack_config = {}
        paystack_config['SECRET_KEY'] = os.getenv("PAYSTACK_SECRET_KEY")
        paystack_config['PUBLIC_KEY'] = os.getenv("PAYSTACK_PUBLIC_KEY")

        if not paystack_config['SECRET_KEY']:
            raise ValueError("Paystack Secret key missing")
        elif not paystack_config['PUBLIC_KEY']:
            raise ValueError('Paystack public key missing')

        return paystack_config

    except Exception as e:
        logger.error(e)
        # raise e


def create_app() -> Sanic:
    """" Application factory """

    app = Sanic("Skrid")

    app.ctx.CLIENT_URL = os.getenv("CLIENT_URL")

    app.config.CORS_ORIGINS = f"{app.ctx.CLIENT_URL}"
    app.config.CORS_SUPPORTS_CREDENTIALS = True
    app.config.CORS_AUTOMATIC_OPTIONS = True
    Extend(app)

    # Load encryption key
    app.ctx.ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY').encode()
        
    fernetkey = Fernet(app.ctx.ENCRYPTION_KEY) 
    app.ctx.fernet = fernetkey

    # Set up magic links
    app.ctx.EmailVerificationLink = SecureMagicLink(
        app=app,
        secret_key=app.ctx.ENCRYPTION_KEY,
        link_type="verify"
    )
    app.ctx.PasswordResetLink = SecureMagicLink(
        app=app,
        secret_key=app.ctx.ENCRYPTION_KEY,
        link_type="reset"
    )

    # Create celery app ctx
    app.ctx.CeleryApp = celeryapp
    app.ctx.tasks ={
        'email_verification': email_verification,
        'email_reset_link': email_reset_link,
        'transaction_pooler': transaction_pooler,
        'trip_task': trip_task
    }

    db_config = load_database_config()
    # app.config.update(db_config)

    # Load email config
    app.ctx.mailConfig = load_mail_config()

    # Set up paystack configuration
    app.ctx.paystackConfig = load_paystack_config()

    # Collection if SSE connections
    app.ctx.SSEClients = set()


    @app.before_server_start
    async def setup(app, loop):
        """ Set up application workers """
        # pprint.pp(app.config)

        # Setup database connection
        dsn = await db_conn(db_config)
        app.ctx.pool = await db_pool(dsn, loop)

        # Set up Scheduler
        # sqlalchemy_engine = create_async_engine(f"postgresql+asyncpg://{db_config['DB_USER']}:{db_config['DB_PASSWORD']}@{db_config['DB_HOST']}:{db_config['DB_PORT']}/{db_config['DB_NAME']}") # Create async SQLAlchemy engine
        
        redis_url = os.getenv("VALKEY_URL") or os.getenv("REDIS_URL")
        jobstore = {
            'default': RedisJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': True,
            'max_imstamces': 3,
            'misfire_grace_time': 60
        }

        app.ctx.scheduler = AsyncIOScheduler(jobstores=jobstore, executors=executors, job_defaults=job_defaults)  # Create scheduler
        app.ctx.scheduler.start()

        # Set up aiohttp ClientSession
        app.ctx.aiohttpClient = aiohttp.ClientSession()

        # Set up templating
        # app.ctx.template_env = await setupTemplating(app)

        # Setup application routing
        await router(app)
        # Register applications
        await register_apps(app)

        await register_services(app)
        await setup_middleware(app)

    @app.after_server_stop
    async def shutdown(app, loop):
        await app.ctx.pool.close()    # Close asyncpg connection pool
        app.ctx.scheduler.shutdown()  # Shutdown APScheduler
        await app.ctx.aiohttpClient.close() # Shutdown AioHTTPClient session

    return app

# if __name__ == "__main__":
    # app.run()

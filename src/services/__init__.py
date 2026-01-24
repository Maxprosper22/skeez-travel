from sanic import Sanic

from .keys import setupECDSAKeys
from .password import PasswordService

from .account import AccountService
from src.models.account import Account

from .trip import TripService
from src.models.trip import Trip, TripStatus, Destination

from .token import TokenService
from src.models.token import Token, TokenType

from .admin import AdminService
from src.models.admin import Admin, AdminRole

from.ticket import TicketService
from src.models.ticket import Ticket, TicketStatus
from .pubsub import PubSub, ClientSubscription, Publisher, Subscriber, Channel, EmailSubscriber, SMSSubscriber, SSESubscriber

async def register_services(app: Sanic) -> None:
    """ Register services """
    
    app.ctx.pubsub = PubSub()
    app.ctx.Publisher = Publisher()
    app.ctx.clientSubscription = ClientSubscription
    app.ctx.Subscriber = Subscriber
    app.ctx.Channel = Channel
    app.ctx.SSESubscriber = SSESubscriber
    app.ctx.SMSSubscriber = SMSSubscriber
    app.ctx.EmailSubscriber = EmailSubscriber

    app.ctx.tripSSEChannel = app.ctx.Publisher.channels['sse']

    app.ctx.tripCtx = {
        'TripService': TripService(app=app, scheduler=app.ctx.scheduler, pool=app.ctx.pool, publisher=app.ctx.Publisher, channel_name='sse'),
        'TripStatus': TripStatus,
        'Trip': Trip,
        'Destination': Destination
    }

    # Create trips table
    await app.ctx.tripCtx["TripService"].create_table(pool=app.ctx.pool)

    # Admin context varuables
    app.ctx.adminCtx = {
        "Admin": Admin,
        "AdminRole": AdminRole,
        "AdminService": AdminService
    }

    # app.ctx.AccountModel = Account
    app.ctx.accountCtx = {
        "Account": Account,
        "AccountService": AccountService(pool=app.ctx.pool, publisher=app.ctx.Publisher)
    }

    # Create new accounts table if it doesn't exist
    await app.ctx.accountCtx["AccountService"].create_table(pool=app.ctx.pool)
    # Create new table table if it doesn't exist
    await app.ctx.adminCtx["AdminService"].create_table(pool=app.ctx.pool)



    await setupECDSAKeys(app)  # Set up encryption keys

    app.ctx.PasswordService = PasswordService

    app.ctx.tokenCtx = {
        "Token": Token,
        "TokenType": TokenType,
        "TokenService": TokenService
    }

    await app.ctx.tokenCtx["TokenService"].create_table(pool=app.ctx.pool)

    app.ctx.ticketCtx = {
        "Ticket": Ticket,
        "TicketStatus": TicketStatus,
        "TicketService": TicketService()
    }

    await app.ctx.ticketCtx["TicketService"].create_table(pool=app.ctx.pool)


    await app.ctx.tripCtx["TripService"].initialise()

    app.add_task(app.ctx.tripCtx['TripService'].run_trips)

    # Retrieve and populate Accounts array of the AccountController
    await app.ctx.accountCtx["AccountService"].initialise(pool=app.ctx.pool)

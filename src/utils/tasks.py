from sanic import Sanic
from sanic.log import logger
from uuid import UUID
from datetime import datetime
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from typing import Optional, List


async def trip_reminder(password: str, messageFrom: str, messageTo: Optional[List[str]], mail_type: str = 'reminder'):
    """ Function for sending trip reminder messages via email """
    try:
        if not messageTo:
            return

        message = EmailMessage()
        message['From'] = messageFrom
        message['TO'] = messageTo
        message['SUBJECT'] = "Trip Reminder"

        if mail_type == 'reminder':
            message.set_content(f"Hi, there. How're you doing today? This is Maxwell from Skeez travel. I wanted to let you know you have a trip coming up tomorrow. Are you set, yet? You can view the trip on the website. Have a nice day")
        else:
            message.set_content(f"Hi, there. Your trip is about to begin. Are you set, yet? You can view the trip on the website. Have a nice day")

        await aiosmtplib.send(
            message,
            hostname="smtp.zoho.com",
            port=587,
            username=messageFrom,
            password=password
        )

    except Exception as e:
        raise e


async def trip_task(tripid: UUID, app: Sanic):
    """ 
        Function that is called whenever a a job is executed by the apscheduler instance. Sends notifications out. Updates trip records if the date matches trip start date
    """
    try:
        tripService = app.ctx.tripCtx['TripService']
        pool = app.ctx.pool
        mail_config = app.ctx.mailConfig
        sse_clients = app.ctx.SSEClients
        
        trip = await tripService.fetch_trip(tripid=tripid)
        if not trip:
            app.ctx.scheduler.remove_job(tripid)
            return

        if datetime.now() < trip.date:
            # Send reminder notifications
            await trip_reminder(
                mail_config['MAIL_PASSWORD'], 
                messageFro=mail_config['ADMIN'], 
                messageTo=[slot.email for slot in trip.slots if trip.slots])
            
            for client in sse_clients:
                for slot in trip.slots:
                    if client[1] == slot.account_id:
                        await client[0].send(f"event: update\ndata: {json.dumps({'message': 'Trip {trip.trip_id} starts in 1 day'})}\n\n")

            return 

        await trip_reminder(
            mail_config['MAIL_PASSWORD'], 
            messageFro=mail_config['ADMIN'], 
            messageTo=[slot.email for slot in trip.slots if trip.slots],
            mail_type="start"
        )
        
        for client in sse_clients:
            for slot in trip.slots:
                if client[1] == slot.account_id:
                    await client[0].send(f"event: update\ndata: {json.dumps({'message': 'Trip {trip.trip_id} is about to commence'})}\n\n")

    except Exception as e:
        logger.error(e)


async def transaction_pooler(app: Sanic, ref: str, tripid: UUID, accountid: UUID):
    """ Pools a Paystack's verify endpoint for transaction status. Runs at regular intervals as a background task """
    try:
        scheduler = app.ctx.scheduler
        ticketStatus = app.ctx.ticketCtx['TicketStatus']
        paystackConfig = app.ctx.paystackConfig
        aiohttpClient = app.ctx.aiohttpClient
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {paystackConfig['SECRET_KEY']}"
        }

        async with app.ctx.aiohttpClient.get(f"https://api.paystack.co/transaction/verify/{ref}", headers=headers) as resp:
            response = resp.json()
            pprint.pp(response)

        match response['data']['status']:
            case "failure" | "abandoned":
                """ Cancel payment processs and delete associated ticket records. Stop this task. """
                await tripService.cancel_booking(tripid, accountid)
                scheduler.remove_job(id=ref)

            case "pending":
                """ Stop this task and retry at another time """
                return

            case "success":
                """ Proceed to complete booking. Stop this task """
                await tripService.complete_booking(ticketStatus.SUCCESS, tripid, accountid)
                scheduler.remove_job(id=ref)
                return

            case _:
                """ Delete records """
                await tripService.cancel_booking(tripid, accountid)
                scheduler.remove_job(id=ref)

    except Exception as e:
        logger.error(e)

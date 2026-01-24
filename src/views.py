from sanic import Sanic, Blueprint
from sanic.request import Request
from sanic.response import html as sanhtml, json as sanjson, text as santext, file as sanfile, HTTPResponse
from sanic.log import logger, error_logger

import pprint
import json
import asyncio
import hmac
import hashlib
import traceback

from uuid import UUID

from src.models.sse import BaseField, Event, Data, ID, Retry, Heartbeat, message

from apscheduler.triggers.interval import IntervalTrigger


async def index(request: Request, path: str):
    app = request.app
    pool = app.ctx.pool

    user = request.ctx.user
    return sanhtml(app.config.DIST_DIR / "index.html")
 

async def view_destinations(request: Request):
    try:
        app = request.app
        pool = app.ctx.pool

        tripCtx = app.ctx.tripCtx
        tripService = tripCtx['TripService']

        destinations = await tripService.fetch_destinations(pool)
        for destination in destinations:
            destination['destination_id'] = destination['destination_id'].hex

        return sanjson(body={'data': destinations if destinations else None})

    except Exception as e:
        logger.error(traceback.format_exc(e))

        return sanjson(status=500, body={"info": "Internal server error"})
    

async def view_trips(request: Request):
    """ 
        View available trips
        Method: Get
    """
    try:
        app = request.app
        pool = app.ctx.pool

        user = request.ctx.user

        rendered_template = template.render(user=user)
        return sanhtml(rendered_template)

    except Exception as e:
        logger.error(traceback.format_exc(e))
        return sanjson(status=500, body={'info': "An umexpected erroe occurred"})


async def fetch_trips(request: Request):
    """ Api endpoint for retrieving trips """
    try:
        app = request.app
        pool = app.ctx.pool

        tripCtx = app.ctx.tripCtx
        Trip = tripCtx['Trip']
        tripStatus = tripCtx['TripStatus']
        tripService = tripCtx['TripService']

        trips = await tripService.fetch(pool=pool)
    
        if not trips:
            # rendered_template = template.render(trips=[])
            return sanjson(body={'info': 'No trip available'})

        all_trips = []
    
        for trip in trips:
            # tripItem = trip.model_dump()

            trip['trip_id'] = trip['trip_id'].hex
            trip['destination_id'] = trip['destination_id'].hex
            trip['destination']['destination_id'] = trip['destination']['destination_id'].hex

            trip['destination']['price'] = int(trip['destination']['price'])
            # trip['status'] = trip['status'].value
            trip['date'] = trip['date'].isoformat()

            for slot in trip['slots']:
                slot['account_id'] = slot['account_id'].hex
                slot['join_date'] = slot['join_date'].isoformat()
                slot.pop('password')

            all_trips.append(trip)
        # pprint.pp(logger.__dict__)
        return sanjson(body={'data': all_trips})

    except Exception as e:
        logger.error(traceback.format_exc(e))
        return sanjson(status=500, body={'info': 'An error occurred'})


async def view_trip(request: Request, tripid: str):
    """ 
        Retrieve data for a specific trip
        Method: Get
    """
    try:
        app = request.app
        pool = app.ctx.pool

        user = request.ctx.user
    
        tripCtx = app.ctx.tripCtx
        tripService = tripCtx['TripService']
    
        if tripid == None:
            return sanjson(401, {"info": "Invalid operation. No trip id provided"})  # A tuple containing a status code and a message: (status, message)
        pprint.pp(f'View trip id: {tripid}')

        tripId = UUID(hex=tripid)
        tripData = await tripService.fetch_trip(tripId)
        if not tripData:
            return sanjson(body={'info': 'Trip not found'})

        tripData = tripData.model_dump()
        tripData['trip_id'] = tripData['trip_id'].hex
        tripData['destination']['destination_id'] = tripData['destination']['destination_id'].hex
        tripData['status'] = tripData['status'].value
        tripData['date'] = tripData['date'].isoformat()
    
        if tripData['slots']:
            for slot in tripData['slots']:
                slot.pop('password')
                slot['account_id'] = slot['account_id'].hex
                slot['join_date'] = slot['join_date'].isoformat()
    
        # pprint.pp(f"Array of trips: {tripData}")
        # passengers = 
    
        return sanjson(body={'data': tripData})

    except Exception as e:
        logger.error(traceback.format_exc(e))
        return sanjson(status=500, body={'info': 'An error occurred'})


async def trip_sse(request: Request, userid: str):
    """Pushes updates to clients via SSE."""
    try:
        app = request.app
        pool = app.ctx.pool

        tripCtx = app.ctx.tripCtx
        Trip = tripCtx['Trip']
        tripStatus = tripCtx['TripStatus']
        tripService = tripCtx['TripService']

        accountCtx = app.ctx.accountCtx
        Account = accountCtx['Account']
        accountService = accountCtx['AccountService']

        # user = request.ctx.user

        sse_clients = app.ctx.SSEClients

        # if userid:
        user = await accountService.fetch_user(accountid=UUID(hex=userid)) if userid else None
            
        
        # Create a response with streaming enabled
        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
        stream = await request.respond(
            headers=headers,
            content_type="text/event-stream"
        )

        client = (stream, user['accountid'] if user else None)

        sse_clients.add(client)
        print(stream)
        pprint.pp(sse_clients)

        # Send initial welcome message
        await stream.send(f"event: welcome\ndata: {json.dumps({'message': 'Connected to trip updates'})}\n\n")

        # Keep connection alive with periodic keep-alive messages
        try:
            while True:
                await stream.send(":\n\n")  # Keep-alive
                await asyncio.sleep(15)  # Every 15 seconds
        except asyncio.CancelledError as e:
            await stream.eof()
            logger.exception("An error occurred")
        except Exception as e:
            await stream.eof()
            logger.exception("An error occurred")
        finally:
            await stream.eof()  # Ensure stream is closed
            sse_clients.discard(stream)


    except Exception:
        logger.exceptas("An error occurred")
        return sanjson(status=500, body={'info': 'An error occurred'})


async def book(request: Request, tripid: str):
    """ 
        Register a passenger for a trip
        Method: Post
    """
    try:
        if not request.token:
            return sanjson(status=401, body={'info': 'Unauthorized'})

        app = request.app
        pool = app.ctx.pool

        paystackConfig = app.ctx.paystackConfig

        aiohttpClient = app.ctx.aiohttpClient

        accountCtx = app.ctx.accountCtx
        accountService = accountCtx['AccountService']

        tripCtx = app.ctx.tripCtx
        Trip = tripCtx['Trip']
        tripStatus = tripCtx['TripStatus']
        tripService = tripCtx['TripService']

        ticketCtx = app.ctx.ticketCtx
        ticketStatus = ticketCtx['TicketStatus']

        scheduler = app.ctx.scheduler
        transaction_task = app.ctx.tasks['transaction_pooler']

        booking_info = request.json

        if 'tripid' not in booking_info:
            return sanjson(status=400, body={'info': 'Bad request'})

        if 'accountid' not in booking_info:
            return sanjson(status=400, body={'info': 'Bad request'})


        trip = await tripService.fetch_trip(tripid=booking_info['tripid'])

        if not trip:
            return sanjson(status=404, body={'info': 'Trip not found'})

        if len(trip.slots) >= trip.capacity:
            return sanjson(status=400, body={'info': 'Trip at full capacity'})

        if trip.date <= datetime.now():
            return sanjson(400, body={'info': 'Booking for this trip is closed'})

        user = await accountService.fetch_user(pool=pool, accountid=UUID(hex=booking_info['accountid']))
        if not user:
            return sanjson(status=403, body={'info': 'Forbidden'})
        
        # Check if user already booked a trip
        async with pool.acquire() as conn:
            ticket = await conn.fetchrow("SELECT * FROM tickets WHERE trip_id=$1 AND account_id=$2", booking_info["tripid"], booking_info["accountid"])

        if ticket:
            return sanjson(status=400, body={'info': 'You already have a reservation'})

        # Initialize transaction
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {paystackConfig['SECRET_KEY']}"
        }
        payload = {
            'email': user['email'],
            'amount': trip.destination.price,
            'metadata': {
                'trip_id': trip.trip_id.hex,
                'account_id': user['account_id'].hex,
                'custom_fields': [
                    {
                        'display_name': 'Trip ID',
                        'variable_name': 'trip_id',
                        'value': trip.trip_id.hex
                    },
                    {
                        'display_name': 'Account ID',
                        'variable_name': 'account_id',
                        'value': user['account_id'].hex
                    }
                 ]
            }
        }

        async with aiohttpClient.post('https://api.paystack.co/transaction/initialize', json=payload, headers=headers) as resp:
            pprint.pp(resp)
            response = await resp.json()
            pprint.pp('Paystack response:')
            pprint.pp(response)

        # Proceed to initialize transaction
        await tripService.book(
            pool,
            UUID(booking_info['tripid']),
            UUID(booking_info['accountid']),
            data=response['data'], 
            status=ticketStatus.PENDING
        )

        # Create a scheduled event that tracks transaction status for failure/abandonment and cancels ticket.
        if trip.date - datetime.now() >= timedelta(days=1):
            interval_trigger = IntervalTrigger(
                minutes=5,
                start_date=datetime.now(),
                end_date=trip.date
            )
        else:
            interval_trigger = IntervalTrigger(
                minutes=5, 
                start_date=datetime.now(),
                end_date=datetime.now() + trip.date - datetime.now()
            )

        # Add a apscheduler job to monitor transaction for failure
        scheduler.add_job(
            transaction_task,
            trigger=interval_trigger,
            id=response['reference'],
            kwargs={
                'app': app,
                'ref': response['data']['reference'],
                'tripid': trip.trip_id,
                'accountid': user['account_id']
            }
        )

        return sanjson(
            status=200,
            body={
                'info': 'Successfully booked trip', 
                'data': {
                    'access_code': response['data']['access_code']
                }
            }
        )

    except Exception as e:
        logger.error(e)
        error_logger.exception("An unexpected error occurred")
        return sanjson(status=500, body={'info': 'An error occurred'})


async def unbook(request: Request, tripid: str):
    """
        Remove a passenger from the list of passengers
        Method: Patch
    """
    try:
        app = request.app
        pool = app.ctx.pool
        tripCtx = app.ctx.tripCtx
        tripService = tripCtx['TripService']

        unbook_data = request.json
        pprint.pp(unbook_data)

        async with pool.acquire() as conn:
            ticket = await conn.fetchrow("SELECT * FROM tickets WHERE trip_id=$1 AND account_id=$2", unbook_data['tripid'], unbook_data['accountid'])

        if not ticket:
            return sanjson(status=400, body={'info': 'No reservation found'})

        unbook_report = await tripService.cancel_booking(
            pool=pool,
            tripid=unbook_data['tripid'],
            accountid=unbook_data['accountid']
        )

        if unbook_report == "FAILURE":
            return sanjson(status=500, body={
                'info': 'An error occurred while processing request'
                })
        return sanjson(status=200, body={'info': "Operation SUCCESS"})

    except Exception as e:
        logger.error(traceback.format_exc(e))
        return sanjson(status=500, body={'info': 'An error occurred'})
    

async def payment_webhook(request: Request):
    """ Webhook for verifying payments """
    try:
        app = request.app
        pool = app.ctx.pool

        paystackConfig = app.cxt.paystackConfig

        tripCtx = app,ctx.tripCtx
        tripService = tripCtx['TripService']

        ticketCtx = app.ctx.ticketCtx
        ticketStatus = ticketCtx['TicketStatus']

        payload = request.body  # Get data
        signature = request.headers.get('x-paystack-signature')

        if not payload or not signature:
            return sanjson(status=400, body={'data': 'Bad request'})

        # Compute HMAC SHA512 hash of payload with secret key
        computed_hash = hmac.new(
            paystackConfig['SECRET_KEY'],
            payload,
            hashlib.sha512
        ).hexdigest()

        # Verify signature
        if not hmac.compare_digest(computed_hash, signature):
            return sanjson(status=400)

        event = request.body

        if event['event'] == "charge.success":
            pprint.pp(event)
            await tripService.complete_booking(pool=pool, status=ticketStatus.SUCCESS)

        else:
            await tripService.cancel_booking(UUID(response['data']['metadata']['trip_id'], UUID(response['data']['metadata']['account_id'])))

        return sanjson(status=200)

    except Exception as e:
        logger.error(traceback.format_exc(e))
        return sanjson(status=500, body={'info': 'An error occurred'})

async def payment_cancel(request: Request):
    """ Endpoint handling cancellation of payment. Deletes created tickets and other associated records """
    try:
        app = request.app
        pool = app.ctx.pool

        return

    except Exception as e:
        logger.error(e)

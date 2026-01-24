import asyncio
from asyncio import PriorityQueue

from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

from sanic import Sanic
from sanic.log import logger

from asyncpg.pool import Pool
from uuid import UUID
from typing import Optional, Dict
from datetime import datetime, timedelta

import pprint
import json

from .pubsub import Publisher, Channel, EmailSubscriber, SMSSubscriber, SSESubscriber

from src.models.trip import TripStatus, Trip, Destination
from src.models.account import Account
from src.models.ticket import Ticket, TicketStatus

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.utils.tasks import trip_task


class EventType(Enum):
    REMINDER = "reminder"
    TRIP_START = "trip_start"


@dataclass(order=True)
class TripEvent:
    event_date: datetime
    event_type: EventType
    trip: Trip


class TripService:

    def __init__(
        self,
        app: Sanic,
        pool: Pool,
        publisher: Publisher,
        channel_name: str,
        scheduler: AsyncIOScheduler
    ):
        self.app = app
        self.trips: Dict[UUID, Trip] = {}
        self.queue = PriorityQueue()
        self.pool: Pool = pool
        self.reminder_offset = timedelta(days=1)
        # self.running: bool = False
        self.scheduler = scheduler   # APScheduler instance
        self.channel: Channel = Channel[channel_name]
        self.publisher: Publisher = publisher


    async def create_table(self, pool: Pool) -> None:
        """ Create a table for storing trips in database """

        try:
            async with pool.acquire() as conn:

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS destinations (
                        destination_id UUID PRIMARY KEY,
                        name TEXT UNIQUE NOT NULL,
                        price NUMERIC NOT NULL
                    )
                """)

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS trips (
                        trip_id UUID PRIMARY KEY,
                        destination_id UUID REFERENCES destinations,
                        capacity INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        date TIMESTAMP NOT NULL
                    )
                """)

                # await conn.execute("""
                #     CREATE TABLE IF NOT EXISTS tickets (
                #         trip_id UUID REFERENCES trips,
                #         account_id UUID REFERENCES accounts,
                #         PRIMARY KEY(trip_id, account_id)
                #     )
                # """)

        except Exception as e:
            raise e


    async def initialise(self) -> None:
        """
            Fetches all trips from database and populate `self.trips`. This method is to be run on server start up.

            Note: To access trips use `self.trips`
        """
        try:
            records = await self.fetch(pool=self.pool)
    
            if not records:
                return

            for trip in records:
                if trip['status'] != TripStatus.ACTIVE.value or trip['status'] != TripStatus.PENDING.value:
                    records.remove(trip)

            for trip_item in records:
                if trip['date'] >= datetime.now():
                    # for slot in trip_item['accounts']:
                    trip = Trip(
                        trip_id = trip_item["trip_id"],
                        destination = trip_item["destination"],
                        capacity = trip_item["capacity"],
                        status = TripStatus(trip_item["status"]),
                        date = trip_item["date"],
                        # slots=SlotList().append(slot) for slot in 
                    )
                    # await self._add_trip_events(trip)
        except Exception as e:
            raise e


    async def _add_trip_events(self, trip: Trip):
        """ Add reminder and trip start events to scheduler/queue """
        try:
            self.trips[trip.trip_id] = trip
            # Add reminder event (1 day before)
            reminder_time = trip.date - self.reminder_offset
            if reminder_time > datetime.now():
                cron_trigger = CronTrigger(start_date=reminder_time, end_date=trip.date)
            else:
                cron_trigger = CronTrigger(end_date=trip.date)

            await self.scheduler.add_job(trip_task, trigger=cron_trigger, kwargs={'tripid': trip.trip_id, 'app': self.app})

        except Exception as e:
            raise e


    # async def create_channel(self, trip: Trip) -> None:
    #     """ Creates a channel for each trip """


    async def create_trip(self, pool: Pool, trip: Trip) -> Optional[dict]:
        """ Create a trip instance """
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO trips (
                        trip_id,
                        destination_id, 
                        capacity,
                        status,
                        date
                    ) VALUES ($1, $2, $3, $4, $5)
                """,
                trip.trip_id, trip.destination.destination_id, trip.capacity, trip.status.value, trip.date)

            await self._add_trip_events(trip)
        
            
    async def update_trip(self, pool: Pool, tripid: UUID, new_start_date: datetime):
        """ Updates trip start date """
        if tripid in self.trips:
            trip = self.trips[tripid]
            trip.date = new_start_date
            # Update database
            async with pool.acquire() as conn:
                await conn.execute("UPDATE trips SET date = $1 WHERE trip_id = $2", new_start_date, tripid)

            # Re-add events to queue
            await self._add_trip_events(trip)


    async def cancel_trip(self, pool: Pool, tripid: UUID) -> Optional[Trip]:
        """ Cancels trip """
        if tripid in self.trips:
            del self.trips[tripid]
            async with pool.acquire() as conn:
                await conn.execute("UPDATE trips SET status = $1 WHERE trip_id=$2", TripStatus.CANCELLED.value, tripid)


    async def complete_trip(self, pool: Pool, tripid: UUID):
        """ Marks a trip as complete """
        
        if tripid in self.trips and self.trips[tripid].status == TripStatus.ACTIVE:
            trip = self.trips[tripid]
            await self.update_trip_status(trip, TripStatus.COMPLETED)
            await self.send_notification(trip, "completed")
            # Optionally remove trip
            del self.trips[tripid]
        else:
            raise ValueError("Trip {tripid} is not active or or does not exist")


    async def update_trip_status(self, pool: Pool, trip: Trip, status: TripStatus):
        """
            Update trip (trip_id) status to (status). Status may be one of the following ['pending', 'active', 'cancelled', 'complete']
        """
        try:
            async with pool.acquire() as conn:
                modded_trip = await conn.execute("""
                    UPDATE trips SET status=$1 WHERE trip_id=$2
                """, status.value, trip.trip_id)

            trip.status = status

        except Exception as e:
            raise e        


    async def book(self, pool: Pool, tripid: UUID, accountid: UUID, status: TicketStatus, data: Dict):
        """ Adds a user to a trip's list """
        try:
            # tripData = await self.fetch_trip(pool, tripid)
            
            if not tripid:
                raise ValueError('Error, trip id not provided')

            if not accountid:
                raise ValueError('Error, account id not provided')
            
            
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO tickets (
                        trip_id, account_id, status, data
                    ) VALUES ($1, $2, $3, $4)""",
                tripid, accountid, status.value, json.dumps(data))
            
        except Exception as e:
            raise e


    async def complete_booking(self, status: TicketStatus, tripid: UUID, accountid: UUID):
        """ Complete a trip booking transaction """
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("UPDATE tickets SET status=$1 WHERE trip_id=$2 AND account_id=$3", status.value, tripid.hex, accountid.hex)
                    ticket = await conn.fetchrow("SELECT * FROM tickets WHERE trip_id=$1 AND account_id=$2", tripid, account_id)

            return ticket if ticket else None

        except Exception as e:
            raise e


    # async def cancel_booking(self, status: TicketStatus, tripid: UUID, accountid: UUID):
    #     """ Cancels and closes booking process after/due to failure """
    #     try:
    #         async with self.pool.acquire() as conn:
    #             await conn.execute("DELETE FROM tickets WHERE trip_id=$1 AND account_id=$2", tripid, accountid)
    #
    #     except Exception as e:
    #         logger.error(e)


    async def cancel_booking(self, tripid: UUID, accountid: UUID):
        """ Removes a user from a trip's list """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""DELETE FROM tickets WHERE trip_id=$1 AND account_id=$2""", tripid, accountid)
                confirm_removal = await conn.fetchrow("SELECT * FROM tickets WHERE trip_id=$1 AND account_id=$2", tripid, accountid)

                if confirm_removal:
                    return "FAILURE"
                return "SUCCESS"
        except Exception as e:
            logger.error(e)


    async def send_notification(self, trip: Trip, event_type: EventType):
        """ Send notifications based on trip events """

        if event_type == EventType.REMINDER:
            print(f"Reminder: Trip {trip.trip_id} starts in 1 day at {trip.date}")
        elif event_type == EventType.TRIP_START:
            print(f"Notification: Trip {trip.trip_id} has started at {trip.date}")


    async def run_trips(self, app: Sanic):
        """
            Run all trips in with pending and active statuses in self.trips. Runs on server startup.
        """
        try:
            while True:
                # Get next event
                trip_event = await self.queue.get()
                current_time = datetime.now()
                event_time = trip_event.event_date
                trip = trip_event.trip

                # Sleep until time for event
                delay = (event_time - current_time).total_seconds()
                if delay > 0:
                    asyncio.sleep(delay)

                # Process all events with the same time
                events_to_process = [trip_event]

                # Pick at the queue to for events with the same time
                while not self.queue.empty():
                    next_event = await self.queue.get()
                    if next_event.event_date <= event_time:
                        events_to_process.append(next_event)
                    else:
                        await self.queue.put(next_event)
                        break

                # Process all collected events
                for event in events_to_process:
                    trip = event.trip
                    if trip.trip_id not in self.trips:
                        continue  # Skip if trip was cancelled or updated
                    if event.event_type == EventType.REMINDER:
                        await self.send_notification(trip, EventType.REMINDER)
                    elif event.event_type == EventType.TRIP_START and trip.status == TripStatus.PENDING:
                        await self.update_trip_status(self.pool, trip, TripStatus.ACTIVE)
                    await self.send_notification(trip, EventType.TRIP_START)

                # Mark all processed events as done
                for _ in events_to_process:
                    self.queue.task_done()

        except Exception as e:
            raise e


    async def fetch(self, pool: Pool) -> Optional[list[Trip]]:
        """ Retrieve all trips """

        try:
            async with pool.acquire() as conn:
                records = await conn.fetch("""
                SELECT 
                    trps.*,
                    dests AS destination,
                    COALESCE(array_agg(accts.*) FILTER (WHERE accts.* IS NOT NULL), '{}') AS slots
                FROM
                    trips trps
                LEFT JOIN
                    destinations dests
                ON 
                    trps.destination_id = dests.destination_id
                LEFT JOIN
                    tickets tkt
                ON 
                    trps.trip_id = tkt.trip_id
                LEFT JOIN
                    accounts accts
                ON
                    tkt.account_id = accts.account_id
                GROUP BY
                    trps.trip_id, dests
                """)

            if not records:
                return None

            tripArray = [dict(record) for record in records]
            for trp in tripArray:
                trp['destination'] = dict(trp['destination'])
                trp['slots'] = [dict(slot) for slot in trp['slots']]

            return tripArray

        except Exception as e:
            raise e


    async def fetch_trip_by_destination(self, destinationid: UUID):
        """ Retrieve trip with matching id """
        try:
            pprint.pp(tripid)
            async with self.pool.acquire() as conn:
                trip_rec = await conn.fetchrow("""
                    SELECT
                        trp.*,
                        row_to_json(dests)::jsonb AS destination,
                        COALESCE(array_agg(acct.*) FILTER (WHERE acct.* IS NOT NULL), '{}') AS slots
                    FROM 
                        trips trp
                    LEFT JOIN
                        destinations dests
                    ON
                        trp.destination_id = dests.destination_id
                    LEFT JOIN
                        tickets tkt 
                    ON
                        trp.trip_id = tkt.trip_id
                    LEFT JOIN
                        accounts acct
                    ON
                        tkt.account_id = acct.account_id
                    WHERE
                        trp.destination_id=$1
                    GROUP BY 
                        trp.trip_id, dests""", 
                destinationid)
            if not trip_rec:
                return None

            return dict(trip_rec)
        except Exception as e:
            logger.error(e)


    async def fetch_trip(self, tripid: UUID=None, destinationid: UUID=None, date: str|datetime=None) -> Optional[Trip]:
        """ Retrieve trip with matching id """
        try:
            pprint.pp(tripid)
            async with self.pool.acquire() as conn:
                if trip_id:
                    trip_rec = await conn.fetchrow("""
                        SELECT
                            trp.*,
                            row_to_json(dests)::jsonb AS destination,
                            COALESCE(array_agg(acct.*) FILTER (WHERE acct.* IS NOT NULL), '{}') AS slots
                        FROM 
                            trips trp
                        LEFT JOIN
                            destinations dests
                        ON
                            trp.destination_id = dests.destination_id
                        LEFT JOIN
                            tickets tkt 
                        ON
                            trp.trip_id = tkt.trip_id
                        LEFT JOIN
                            accounts acct
                        ON
                            tkt.account_id = acct.account_id
                        WHERE
                            trp.trip_id=$1
                        GROUP BY 
                            trp.trip_id, dests""", 
                    tripid)
                elif destinationid:
                    trip_rec = await self.fetch_destination(destinationid)

                elif date:
                    if type(date) == str:
                        date = datetime(date)

                    trip_rec = await conn.fetchrow("""
                        SELECT
                            trp.*,
                            row_to_json(dests)::jsonb AS destination,
                            COALESCE(array_agg(acct.*) FILTER (WHERE acct.* IS NOT NULL), '{}') AS slots
                        FROM 
                            trips trp
                        LEFT JOIN
                            destinations dests
                        ON
                            trp.destination_id = dests.destination_id
                        LEFT JOIN
                            tickets tkt 
                        ON
                            trp.trip_id = tkt.trip_id
                        LEFT JOIN
                            accounts acct
                        ON
                            tkt.account_id = acct.account_id
                        WHERE
                            trp.date=$1
                        GROUP BY 
                            trp.trip_id, dests""", 
                    date)
    
            if not trip_rec:
                return None

            pprint.pp(trip_rec)

            tripDict = dict(trip_rec)
            tripDict['destination'] = json.loads(tripDict['destination'])
            tripDict['status'] = TripStatus(tripDict['status'])
            # tripDict['slots'] = [dict(slot) for slot in tripDict['slots'] if tripDict['slots']]

            trip = Trip(
                trip_id = tripDict['trip_id'],
                destination=tripDict['destination'],
                capacity=tripDict['capacity'],
                status=tripDict['status'],
                date=tripDict['date'],
                slots = [
                    Account(
                        account_id = slot['account_id'],
                        email = slot['email'],
                        phone_number = slot['phone_number'],
                        password = slot['password'],
                        firstname = slot['firstname'],
                        lastname = slot['lastname'],
                        othername = slot['othername'],
                        join_date = slot['join_date'],
                        is_admin = slot['is_admin']
                    ) for slot in tripDict['slots'] if tripDict['slots']
                ]
            )

            return trip

        except Exception as e:
            logger.error(e)



    async def create_destination(self, pool: Pool, destination: Destination):
        try:
            if not destination:
                return
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO destinations (destination_id, name, price) VALUES ($1, $2, $3)
                """, destination.destination_id, destination.name, destination.price)
            record = await self.fetch_destination(pool, destination.name)

            if not record:
                return None
            return dict(record)
        except Exception as e:
            raise e


    async def fetch_destination(self, pool: Pool, title: str):
        try:
            async with pool.acquire() as conn:
                record = await conn.fetchrow("""SELECT * FROM destinations WHERE name=$1""", title)

            if not record:
                 return None
            return dict(record)

        except Exception as e:
            raise e


    async def fetch_destinations(self, pool: Pool):
        """ Fetch registered destinations """

        try:
            async with pool.acquire() as conn:
                records = await conn.fetch("SELECT * FROM destinations")
            if not records:
                return None

            return [dict(record) for record in records]
        except Exception as e:
            raise e


    # @classmethod
    # async def pending_trip_task(cls, app: Sanic, trip: Trip) -> None:
    #     """ A method tracks the approaching date of a trip """
    #     try:
    #         count_down = trip.date - datetime.now()
    #         if datetime.now() >= trip.date:
    #             """
    #                 - Send alert to admin and users about the start of trip 
    #                 - Change trip status to active
    #             """
    #             cls.update_trip_status(
    #                 cls.pool,
    #                 trip=trip.trip_id,
    #                 status=TripStatus.ACTIVE
    #             )
    #             cls.app.cancel_task(name=f"trip-{trip.trip_id.hex}")
    #
    #         if count_down.days != 0 and count_down.days <= 3:
    #
    #             await asyncio.sleep(86400) # Sleep for 24 hours and check again
    #             # Send email and sms
    #             print(f"Hello there! Your trip to {trip.destination} is 5 days away. How's your packing going?")
    #
    #     except Exception as e:
    #         raise e
    #



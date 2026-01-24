from sanic import Sanic
from sanic.response import html as sanhtml, text as santext, json as sanjson
from sanic.request import Request
from sanic.views import HTTPMethodView
from sanic.log import logger
# from returns.maybe import Maybe, Some, Nothing

from apscheduler.triggers.cron import CronTrigger

import pprint


async def view_destinations(request: Request):
    try:
        app = rrquest.app
        pool = app.ctx.pool

        tripCtx = app.tripCtx
        tripService = tripCtx['TripService']

        destinations = await tripService.fetch_destinations(pool)

        return sanjson(body={'data': destinations if destinations else None})
    except Exception as e:
        logger.error(e)


async def new_destination(request: Request):
    try:
        app = request.app
        pool = app.ctx.pool
        tripCtx = app.ctx.tripCtx
        tripService = tripCtx['TripService']

        form_data = request.json
        pprint.pp(form_data)
        destination = tripCtx['Destination'](name=form_data['name'], price=form_data['price'])

        exists = await tripService.fetch_destination(pool, destination.name)
        if exists:
            return sanjson(body={'info': 'Destination exists'})
        data = await tripService.create_destination(pool, destination)
        data['destination_id'] = data['destination_id'].hex

        return sanjson(body={'data': data})
    except Exception as e:
        logger.error(e)


async def view_accounts(request: Request):
    """ View and manage accounts """
    try:
        app = request.app
        pool = app.ctx.pool
        templateEnv = app.ctx.template_env
        template = templateEnv.get_template('admin/accounts.html')

        accountCtx = app.ctx.accountCtx
        Account = accountCtx['Account']
        accountService = accountCtx['AccountService']

        accounts = await accountService.fetch(pool)
    
        if not accounts:
            return sanhtml(template.render(accounts=[]))

        pprint.pp(accounts)
   
        for account in accounts:
            account['account_id'] = account['account_id'].hex
            account['join_date'] = account['join_date'].isoformat()

            if account['admin']:
                account['admin']['admin_id'] = account['admin']['admin_id'].hex
                account['admin']['account_id'] = account['admin']['account_id'].hex
                # account['admin']['role'] = account['admin']['role']
                account['admin']['date'] = account['admin']['date'].isoformat()
            
            if account['trips']:
                for trip in account['trips']:
                    trip['trip_id'] = trip['trip_id'].hex
                    trip['date'] = trip['date'].isoformat()

        return sanhtml(template.render(accounts=accounts))

    except Exception as e:
        logger.error(e)


async def fetch_accounts(request: Request):
    """ 
        Retrieve data on accounts
        Method: Get
    """
    try:
        app = request.app
        pool = app.ctx.pool

        accountCtx = app.ctx.accountCtx
        Account = accountCtx['Account']
        accountService = accountCtx['AccountService']

        accounts = await accountService.fetch(pool)
    
        if not accounts:
            return sanjson(body={'data': []})
   
        for account in accounts:
            account['account_id'] = account['account_id'].hex
            account['join_date'] = account['join_date'].isoformat()

            if account['admin']:
                account['admin']['admin_id'] = account['admin']['admin_id'].hex
                account['admin']['account_id'] = account['admin']['account_id'].hex
                # account['admin']['role'] = account['admin']['role']
                account['admin']['date'] = account['admin']['date'].isoformat()
            
            if account['trips']:
                for trip in account['trips']:
                    trip['trip_id'] = trip['trip_id'].hex
                    trip['date'] = trip['date'].isoformat()

        # pprint.pp(trips)
        return sanjson(body={'data': accounts})

    except Exception as e:
        logger.error(e)


async def view_trips(request: Request):
    """ Loads trip data """

    try:
        app = request.app
        pool = app.ctx.pool
        templateEnv = app.ctx.template_env
        template = templateEnv.get_template('main/trips.html')

        return sanhtml(template.render())

    except Exception as e:
        logger.error(e)


async def fetch_trips(request: Request):
    """ 
        Retrieve data on available trips
        Method: Get
    """
    try:
        app = request.app
        pool = app.ctx.pool
        tripCtx = app.ctx.tripCtx
        Trip = tripCtx['Trip']
        tripStatus = tripCtx['TripStatus']
        tripService = tripCtx['TripService']

        trips = await tripService.fetch(pool)
    
        if not trips:
            return sanjson(body={'data': []})
   
        for trip in trips:
            trip['trip_id'] = trip['trip_id'].hex
            trip['date'] = trip['date'].isoformat()
            
            if trip['slots']:
                for slot in trip['slots']:
                    slot['account_id'] = slot['account_id'].hex
                    slot['join_date'] = slot['join_date'].isoformat()

        # pprint.pp(trips)
        return sanjson(body={'data': trips})

    except Exception as e:
        logger.error(e)


async def create_trip(request: Request):
    """ 
        Handler for creating new trips.

        Note;
            [1]: Update code to directly search db for trip that matches the given data rather than fetch all trips and then iteratung over them.
    """
    try:
        app = request.app       # The Sanic app instance
        # print('Application blueprints: ', app.blueprints)
        # bp = app.bl
        pool = app.ctx.pool     # Database connection pool
        tripCtx = app.ctx.tripCtx
        tripService = tripCtx["TripService"]    # TripController class instance
        tripStatus = tripCtx['TripStatus']    # Trip status enum class

        Trip = tripCtx['Trip']     # Trip class object (not instance!)
    
        scheduler = app.ctx.scheduler
        sse_clients = app.ctx.SSEClients

        trip_data = request.json    # Form data submitted
        pprint.pp(trip_data)

        match trip_data:
            case None:
                return sanjson(
                    status=400,
                    body={
                        'message': "Bad request. No data provided"
                    }
                )
            case {'destination': t_dest, 'date': t_date, 'capacity': t_cap, 'status': t_stat} if t_dest == None or t_date == None or t_cap == None or t_stat == None:
                return sanjson(
                    status=400,
                    body={
                        'message': 'Bad request. Missing required fields'
                    }
                )
    
        match trip_data['status']:
            case 'pending':
                trip_data['status'] = tripStatus.PENDING
            case 'active':
                trip_data['status'] = tripStatus.ACTIVE

        print(trip_data)

        trips = tripService.trips 
    
        # See Note [1] for the code below

        trip = await tripService.fetch_trip()
        if len(trips) >= 1:
            match trips:
                case [Trip as trip] if trip['destination']==trip_data['destination'] and trip['status']==tripStatus.PENDING and trip['date']==trip_data['date'] and len(trip['slot']) < trip['capacity']:
                    # print(trip)
                    # await 

                    return sanjson(
                        status=403,
                        body={
                            'status': 'err',
                            "msg": f"Unable to create trip to '{trip_data.destination}'. Existing trip(s) with free slots found"
                    })

        new_trip = Trip(
            destination=trip_data['destination'],
            date=trip_data['date'],
            status=trip_data['status'],
            capacity=trip_data['capacity']
        )
        print(new_trip)

        await tripService.create_trip(pool, new_trip)
        saved_trip = await tripService.fetch_trip(new_trip.trip_id)
        print("New Trip: ", saved_trip)
    
        if not saved_trip:
            return sanjson(
                status=404, 
                body={
                    'status': 'bad',
                    'msg': 'Trip not created'
                }
            )

        alarm_date = saved_trip.date - timedelta(days=2)
        scheduler.add_job(
            trip_task,
            trigger=CronTrigger(
                start_date=alarm_date,
                end_date=trip.date
            ),
            app=app,
            id=saved_trip.trip_id.hex,
            coalesce=True,
            replace_existing=True
        )

        trip_dict = saved_trip.model_dump()
        if type(trip_dict['trip_id']) == UUID:
            trip_dict['trip_id'] = trip_dict['trip_id'].hex
        if type(trip_dict['destination']['destination_id']) == UUID:
            trip_dict['destination']['destination_id'] = trip_dict['destination']['destination_id'].hex
        if trip_dict['accounts']:
            for account in trip_dic['accounts']:
                if type(account['account_id']) == UUID:
                    account['account_id'] = account['account_id'].hex

        for client in sse_clients:
            client[0].send(f"event: notice\ndata: {json.dumps({'message': 'New Trip created', 'data': trip_dict})}\n\n")
        
        # Send signal to websocket hanlder to update trip listing 
        return sanjson(
            status = 201, 
            body = {
               'status': 'ok',
               'msg': 'Trip created',
               'data': {'trip': saved_trip}
            }
        )

    except Exception as e:
        logger.error(e)
           

async def start_trip(request: Request, trip_id: str):
    """
        Endpoint to begin a specific trip
        Method: Patch
    """


async def end_trip(request: Request, trip_id: str):
    """ 
        Handles trip completion
        Method: Get
    """


async def cancel_trip(request: Request, trip_id: str):
    """ 
        Cancels trip before completion
        Method: Put
    """


class TripView(HTTPMethodView):
    async def get(self, request: Request, trip_id: str):
        """ Fetch a specific trip """


    async def post(self, request: Request):
        """ Create a new trip """


    async def delete(self, request: Request, trip_id: str):
        """ Delete a trip """


    async def patch(self, request: Request, trip_id, **kwargs):
        """ Update trip data """


    async def put(self, request: Request):
        """ Update trip """


async def new_admin(request: Request) -> sanjson:
    """ Create new admin accounts """

    try:
        app = request.app
        pool = app.ctx.pool

        passwordService = app.ctx.PasswordService

        accountCtx = app.ctx.accountCtx
        accountService = accountCtx["AccountService"]
        account = accountCtx["Account"]

        adminCtx = app.ctx.adminCtx

        new_admin_data = request.json
        pprint.pp(f"New Admin Data: {new_admin_data}")

        if not new_admin_data:
            return sanjson(status=400, body={'info': 'Missing required data'})
        if 'accountid' in new_admin_data and 'role' in new_admin_data:
            account = await accountService.fetch_user(pool=pool, accountid=new_admin_data['accountid'])
            if account:
                if acct.is_admin:
                    return sanjson(body={
                        "info": "User is already admin"
                    })

                newAdmin = adminCtx["Admin"](
                    account_id=acct.account_id,
                    role=adminCtx["AdminRole"](role).value
                )
                acct.admin=newAdmin

                async with pool.acquire() as conn:
                    await conn.execute("""INSERT INTO admin (
                        admin_id,
                        account_id,
                        role,
                        date
                    ) VALUES ($1, $2, $3, $4)""", newAdmin.admin_id, newAdmin.account_id, newAdmin.role.value, newAdmin.date)

                new_admin = dict(newAdmin)
                new_admin['admin_id'] = new_admin['admin_id'].hex
                new_admin['account_id']  = new_admin['account_id'].hex
                new_admin['role'] = new_admin['role'].value
                new_admin['date'] = new_admin['date'].isoformat()

                return sanjson(status=201, body={
                    'info': 'Operation succesful',
                    'data': new_admin
                })
    
        elif 'email' in new_admin_data and 'role' in new_admin_data:
            account = await accountService.fetch_user(pool=pool, email=new_admin_data['email'])
            print(f"All accounts data:")
            pprint.pp(account)
            if account['is_admin']:
                return sanjson(body={
                    "info": "User is already admin"
                })

            newAdmin = adminCtx["Admin"](
                account_id=account['account_id'],
                role=adminCtx["AdminRole"](new_admin_data['role']).value
            )
            account['admin']=newAdmin.model_dump()

            async with pool.acquire() as conn:
                await conn.execute("""INSERT INTO admin (
                    admin_id, 
                    account_id, 
                    role, 
                    date
                ) VALUES ($1, $2, $3, $4)""", newAdmin.admin_id, newAdmin.account_id, newAdmin.role.value, newAdmin.date)
                await conn.execute("""
                    UPDATE accounts SET is_admin=true WHERE email=$1
                """, new_admin_data['email'])
    
            new_admin = dict(newAdmin)
            new_admin['admin_id'] = new_admin['admin_id'].hex
            new_admin['account_id']  = new_admin['account_id'].hex
            new_admin['role'] = new_admin['role'].value
            new_admin['date'] = new_admin['date'].isoformat()

            return sanjson(status=201, body={'info': 'Operation succesful', 'data': new_admin})

    except Exception as e:
        raise e


async def signin(request: Request):
    """ Admin authentication endpoint """
    app = request.app
    pool = app.ctx.pool
    accountService = app.ctx.accountCtx["AccountService"]
    passwordService = app.ctx.PasswordService
    
    pprint.pp(accountService.accounts)
    auth_data = request.json
    print(auth_data)

    match auth_data:
        case {'email': email, 'password': password}:

            match accountService.accounts:
                case [Account as acc] if acc.email == email:
                    match acc.is_admin:
                        case True:
                            passCheck = await passwordService.pwd_context.verify(
                                password, acc.password
                            )
                            match passCheck:
                                case True:
                                    response = redirect("admin/dashboard")
                                    # Generate auth token
                                    account_details = dict(acc)
                                    account_details.pop('password')
                                    return sanjson(
                                        status=200,
                                        body={
                                            'info': 'Success',
                                            'data': {
                                                'account': account_details,
                                                'token': token
                                            }
                                    })

                                case False:
                                    return sanjson(
                                        status=401,
                                        body={
                                            'msg': 'Incorrect credentials'
                                        })
                        case False:
                            return sanjson(status=403, body={
                                'status': 'error',
                                'msg': 'Forbidden'
                            })
                case _:
                    return sanjson(
                        status=404,
                        body={
                            'message': 'Account not recognised'
                    })
        case _:
            return sanjson(
                status=400,
                body={
                    'msg': 'Bad request. Credentials not provided'
            })


async def admin_ws(request, ws):
    """ Admin websocket endpoint """


async def dashboard(request: Request):
    """ Admin dashboard """

    app = request.app
    pool = app.ctx.pool
    templateEnv = app.ctx.template_env
    
    print(type(templateEnv))
    print(templateEnv.loader.__dir__())
    print(templateEnv.loader.list_templates)

    template = templateEnv.get_template("admin/dashboard.html").render()

    return sanhtml(template)

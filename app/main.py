from aiohttp import web
import aiosqlite
import json
import asyncio

HOST = '0.0.0.0'
PORT = 26500
DATABASE_PATH = 'mydatabase.db'
HOLD_UPDATE_INTERVAL = 600


class UserNotFound(Exception):
    pass


class OperationNotPossible(Exception):
    pass


def subtraction_is_possible(balance, hold, value):
    return hold + value <= balance


async def get_user_data_by_uuid(conn, uuid):
    cur = await conn.execute('SELECT * FROM users WHERE uuid=?', (uuid,))
    row = await cur.fetchone()
    if row is None:
        raise UserNotFound('uuid not found')
    uuid, name, balance, hold, status = row
    user_data = {
        'uuid': uuid,
        'name': name,
        'balance': balance,
        'hold': hold,
        'status': bool(status),
    }
    return user_data


async def update_user_data(conn, uuid, params):
    keys = []
    values = []
    for key, value in params.items():
        keys.append(key)
        values.append(value)
    sql = '''UPDATE users 
             SET {}
             WHERE uuid=?'''.format(', '.join(f'{key}=?' for key in keys))
    await conn.execute(sql, values + [uuid])
    await conn.commit()


def make_response(status, result, addition, description):
    response_obj = {
        'status': status,
        'result': result,
        'addition': addition,
        'description': description
    }
    return web.json_response(response_obj)


def make_user_not_found_response(err):
    status = 404
    result = False
    addition = {
        'reason': str(err)
    }
    description = {}
    return make_response(status, result, addition, description)


def make_bad_request_response(err):
    status = 400
    result = False
    addition = {
        'reason': 'bad request'
    }
    description = {}
    return make_response(status, result, addition, description)


def make_operation_not_possible_response(err):
    status = 403
    result = False
    addition = {
        'reason': str(err)
    }
    description = {}
    return make_response(status, result, addition, description)


async def handle_ping(request):
    status = 200
    result = True
    addition = {}
    description = {}
    return make_response(status, result, addition, description)


async def handle_status(request):
    request_text = await request.text()
    try:
        request_data = json.loads(request_text)
        uuid = request_data['addition']['uuid']
    except (KeyError, json.JSONDecodeError) as e:
        return make_bad_request_response(e)
    conn = request.app['conn']
    try:
        user_data = await get_user_data_by_uuid(conn, uuid)
    except UserNotFound as e:
        return make_user_not_found_response(e)
    status = 200
    result = True
    addition = {
        'balance': user_data['balance'],
        'hold': user_data['hold'],
        'status': user_data['status'],
    }
    description = {}
    return make_response(status, result, addition, description)


async def handle_add(request):
    request_text = await request.text()
    try:
        request_data = json.loads(request_text)
        uuid = request_data['addition']['uuid']
        value = request_data['addition']['value']
        assert isinstance(value, int) and value >= 0
    except (KeyError, json.JSONDecodeError, AssertionError) as e:
        return make_bad_request_response(e)
    conn = request.app['conn']
    lock, counter = request.app['locked_rows'].get(uuid, (asyncio.Lock(), 0))
    request.app['locked_rows'][uuid] = (lock, counter+1)
    try:
        async with lock:
            user_data = await get_user_data_by_uuid(conn, uuid)
            if not user_data['status']:
                raise OperationNotPossible('status is inactive')
            balance = user_data['balance']
            balance += value
            await update_user_data(conn, uuid, {'balance': balance})
    except UserNotFound as e:
        return make_user_not_found_response(e)
    except OperationNotPossible as e:
        return make_operation_not_possible_response(e)
    finally:
        lock, count = request.app['locked_rows'][uuid]
        if count == 1:
            del request.app['locked_rows'][uuid]
        else:
            request.app['locked_rows'][uuid] = (lock, count-1)
    status = 200
    result = True
    addition = {}
    description = {}
    return make_response(status, result, addition, description)


async def handle_subtract(request):
    request_text = await request.text()
    try:
        request_data = json.loads(request_text)
        uuid = request_data['addition']['uuid']
        value = request_data['addition']['value']
        assert isinstance(value, int) and value >= 0
    except (json.JSONDecodeError, KeyError, AssertionError) as e:
        return make_bad_request_response(e)
    conn = request.app['conn']
    lock, count = request.app['locked_rows'].get(uuid, (asyncio.Lock(), 0))
    request.app['locked_rows'][uuid] = (lock, count+1)
    try:
        async with lock:
            user_data = await get_user_data_by_uuid(conn, uuid)
            if not user_data['status']:
                raise OperationNotPossible('status is inactive')
            balance = user_data['balance']
            hold = user_data['hold']
            if not subtraction_is_possible(balance, hold, value):
                raise OperationNotPossible('balance too low')
            hold += value
            await update_user_data(conn, uuid, {'hold': hold})
    except UserNotFound as e:
        return make_user_not_found_response(e)
    except OperationNotPossible as e:
        return make_operation_not_possible_response(e)
    finally:
        lock, count = request.app['locked_rows'][uuid]
        if count == 1:
            del request.app['locked_rows'][uuid]
        else:
            request.app['locked_rows'][uuid] = (lock, count-1)
    status = 200
    result = True
    addition = {}
    description = {}
    return make_response(status, result, addition, description)


async def auto_update_hold(conn):
    while True:
        await asyncio.sleep(HOLD_UPDATE_INTERVAL)
        sql = 'UPDATE users SET balance=balance-hold, hold=0'
        await conn.execute(sql)
        await conn.commit()


async def main():
    app = web.Application()
    app.router.add_routes([
        web.post('/api/ping', handle_ping),
        web.post('/api/status', handle_status),
        web.post('/api/add', handle_add),
        web.post('/api/subtract', handle_subtract)
    ])
    conn = await aiosqlite.connect(DATABASE_PATH)
    app['conn'] = conn
    app['locked_rows'] = {}
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await asyncio.gather(site.start(), auto_update_hold(conn))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

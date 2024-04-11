from telescraper import scraper
import pytest
import asyncio

import datetime
import io
from dateutil.tz import *

pytestmark = pytest.mark.asyncio(scope="module")

loop: asyncio.AbstractEventLoop

# cancel all running tasks
def cancel_all_tasks(exclude_current=False):
    # get all tasks
    all_tasks = asyncio.all_tasks()
    # check if we should not cancel the current task
    if exclude_current:
        # exclude the current task if needed
        all_tasks.remove(asyncio.current_task())
    # enumerate all tasks
    for task in all_tasks:
        # request the task cancel
        task.cancel()

@pytest.mark.parametrize('ini_file', ['../config.ini'])
async def test_grouped_media(ini_file):
    """ Test groupped media """
    # periods = [(datetime.datetime(2023, 2, 23, 17, 00), datetime.datetime(2023, 2, 23, 18, 00))]
    periods = [(datetime.datetime(2022, 2, 27, 5, 32).astimezone(tzutc()), datetime.datetime(2022, 2, 27, 5, 33).astimezone(tzutc()))]

    usr = "https://t.me/imnotbozhena"
    msg_id = None
    # assert 1 == 1
    # monkeypatch.setattr('sys.stdin', io.StringIO('+18128372006'))    
    client = await scraper.config_session(ini_file, "../'alex192838'.session")
    channel_info = await scraper.get_channel(usr, client)
    my_channel = await client.get_entity(usr)
    count = 0
    for period in periods:
        print("----------------------------------------------------------")
        messages = await scraper.scrape_messages(period, client, my_channel, msg_id)
        for msg in messages:
            for med in msg['media']:
                count+=1
    assert count == 4

    # Wait for a disconnection to occur
    client.disconnect()
    while client.is_connected():
        await asyncio.sleep(0.1)

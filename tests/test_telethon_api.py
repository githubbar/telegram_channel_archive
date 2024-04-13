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

@pytest.mark.parametrize(
        "usr, msgids, imgCount", [
            ("https://t.me/imnotbozhena", [23421, 23422,23423,23424], 4),
            ("https://t.me/warfakes", [3, 4], 2),
            ]
    )
async def test_grouped_media(usr, msgids, imgCount):
    """ Test groupped media """
    client = await scraper.config_session()
    channel_info = await scraper.get_channel(client, usr)
    my_channel = await client.get_entity(usr)
    count = 0
    msg = await scraper.scrape_messages(client, my_channel, ids=msgids)
    # period = (datetime.datetime(2022, 2, 27, 5, 32).astimezone(tzutc()), datetime.datetime(2022, 2, 27, 5, 33).astimezone(tzutc()))
    # msg = await scraper.scrape_messages(client, my_channel, period = period)
    print(msg)
    for m in msg:
        for med in m['media']:
            count+=1
    try:
        assert count == imgCount
    finally:
        # Wait for a disconnection to occur
        client.disconnect()
        while client.is_connected():
            await asyncio.sleep(0.1)

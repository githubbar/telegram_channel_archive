#!/usr/bin/env python
# coding: utf-8
import os
# The environment complains without it
os.environ['OPENBLAS_NUM_THREADS'] = '1'
from telescraper import scraper
from telethon import TelegramClient
import datetime
from dateutil.tz import *
import sqlite3
8128372006
# https://t.me/activatica/30924?comment=6828

# Take first 100 days since channels were started
skipToChannelNumber = 0 # Temporary
channel_list = ["https://t.me/femagainstwar", "https://t.me/mpartisans", "https://t.me/zelenayalenta"]
# starts = [datetime.datetime(2022, 2, 25, 0, 0), datetime.datetime(2022, 3, 15, 0, 0), datetime.datetime(2022, 2, 27, 0, 0)]
starts = [datetime.datetime(2023, 2, 25, 0, 0), datetime.datetime(2023, 3, 15, 0, 0), datetime.datetime(2023, 2, 27, 0, 0)]
periods = [(start, start + datetime.timedelta(days=100)) for start in starts]

async def main():
    import traceback
    client = await scraper.config_session()
    con = scraper.config_db()
    
    try:
        for n, usr in enumerate(channel_list):
            if n < skipToChannelNumber:
                continue
            channel_info = await scraper.get_channel(client, usr)
          
            scraper.save_channel_to_db(channel_info, con) 
            my_channel = await client.get_entity(usr)
            
            period = periods[n]
            print("----------------------------------------------------------")
            messages = await scraper.scrape_messages(client, my_channel, period=period)
           
            scraper.save_messages_to_db(messages, con) 

            # break
            
    except Exception as e:
        traceback.print_exc()
    finally:        
        con.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())  
    
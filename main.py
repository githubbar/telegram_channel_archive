#!/usr/bin/env python
# coding: utf-8
from telescraper import scraper
from telethon import TelegramClient
import datetime
from dateutil.tz import *
import sqlite3

# https://t.me/activatica/30924?comment=6828

# decided periods to scrape    
skipToChannelNumber = 1
# take first 100 days since channels were started
# starts = [datetime.datetime(2023, 2, 25, 0, 0), datetime.datetime(2023, 3, 15, 0, 0), datetime.datetime(2023, 2, 27, 0, 0)]
# BEGIN TEMP
# TODO: fix https://t.me/mpartisans/27 not saving to DB
starts = [datetime.datetime(2022, 2, 25, 0, 0), datetime.datetime(2022, 3, 21, 0, 0), datetime.datetime(2022, 2, 27, 0, 0)]
periods = [(start, start + datetime.timedelta(days=1)) for start in starts]
# END TEMP
# periods = [(start, start + datetime.timedelta(days=100)) for start in starts]
# starts = [datetime.datetime(2022, 3, 14, 0, 0), datetime.datetime(2022, 3, 15, 0, 0), datetime.datetime(2022, 2, 27, 0, 0)]
# periods = [(start, start + datetime.timedelta(days=1)) for start in starts]
    
# periods = [(datetime.datetime(2022, 2, 24, 6, 2).astimezone(tzutc()), datetime.datetime(2022, 2, 24, 6, 3).astimezone(tzutc()))]

# periods = [(datetime.datetime(2022, 2, 28, 20, 2), datetime.datetime(2022, 2, 28, 20, 4))]
# periods = [(datetime.datetime(2022, 2, 28, 15, 21), datetime.datetime(2022, 2, 28, 15, 23))]

channel_list = ["https://t.me/femagainstwar", "https://t.me/mpartisans", "https://t.me/zelenayalenta"]

# channel_list = ["https://t.me/warfakes"]
# channel_list = ["https://t.me/imnotbozhena"]
# channel_list = ["https://t.me/activatica"]
# channel_list = ["https://t.me/femagainstwar"]
# channel_list = ["https://t.me/femagainstwar"]

# msg_id = 24574
# msg_id = 30924
# msg_id = 30917
# msg_id = None

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

            con.commit()
            break
            
    except Exception as e:
        traceback.print_exc()
    finally:        
        con.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())  
    
#!/usr/bin/env python
# coding: utf-8
from telescraper import scraper
from telethon import TelegramClient
import datetime
from dateutil.tz import *
import sqlite3

# https://t.me/activatica/30924?comment=6828

# decided periods to scrape    
start1 = datetime.datetime(2022, 2, 25, 0, 0)
periods = [(start1, start1 + datetime.timedelta(days=1))]
# periods = [(datetime.datetime(2022, 2, 24, 6, 2).astimezone(tzutc()), datetime.datetime(2022, 2, 24, 6, 3).astimezone(tzutc()))]
# periods = [(datetime.datetime(2023, 2, 23, 17, 00), datetime.datetime(2023, 2, 23, 18, 00))]
# periods = [(datetime.datetime(2022, 2, 27, 5, 32).astimezone(tzutc()), datetime.datetime(2022, 2, 27, 5, 33).astimezone(tzutc()))]
# periods = [(datetime.datetime(2022, 2, 28, 20, 2), datetime.datetime(2022, 2, 28, 20, 4))]
# periods = [(datetime.datetime(2022, 2, 28, 15, 21), datetime.datetime(2022, 2, 28, 15, 23))]

channel_list = ["https://t.me/femagainstwar"]
# channel_list = ["https://t.me/warfakes"]
# channel_list = ["https://t.me/imnotbozhena"]
# channel_list = ["https://t.me/activatica"]
# channel_list = ["https://t.me/imnotbozhena"]
# channel_list = ["https://t.me/femagainstwar"]
# channel_list = ["https://t.me/femagainstwar"]

# msg_id = 24574
# msg_id = 30924
# msg_id = 30917
# msg_id = None

async def main():
    import traceback
    client = await scraper.config_session()
    ch_list = [channel_list[0]]
    con = scraper.config_db()
    
    try:
        for usr in ch_list:
            channel_info = await scraper.get_channel(usr, client)
            scraper.save_channel_to_db(channel_info, con) 
            my_channel = await client.get_entity(usr)
            for period in periods:
                print("----------------------------------------------------------")
                messages = await scraper.scrape_messages(period, client, my_channel)
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
    
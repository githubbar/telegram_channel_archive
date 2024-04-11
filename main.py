#!/usr/bin/env python
# coding: utf-8
from telescraper import scraper
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import datetime
from dateutil.tz import *
import sqlite3

# https://t.me/activatica/30924?comment=6828

# decided periods to scrape    
# periods = [(None, None)]
periods = [(datetime.datetime(2023, 2, 23, 17, 00), datetime.datetime(2023, 2, 23, 18, 00))]
# periods = [(datetime.datetime(2022, 2, 27, 5, 32).astimezone(tzutc()), datetime.datetime(2022, 2, 27, 5, 33).astimezone(tzutc()))]
# periods = [(datetime.datetime(2022, 2, 28, 20, 2), datetime.datetime(2022, 2, 28, 20, 4))]
# periods = [(datetime.datetime(2022, 2, 28, 15, 21), datetime.datetime(2022, 2, 28, 15, 23))]

channel_list = ["https://t.me/activatica"]
# channel_list = ["https://t.me/imnotbozhena"]
# channel_list = ["https://t.me/femagainstwar"]
# channel_list = ["https://t.me/femagainstwar"]

# msg_id = 30924
# msg_id = 30917
msg_id = None

async def config_session(inifile="config.ini"):
    config = scraper.configparser.ConfigParser()
    config.read(inifile)

    # setting configuration values
    api_id = config.get('Telegram', 'api_id')
    api_hash = config.get('Telegram', 'api_hash')
    phone = config.get('Telegram', 'phone')
    username = config.get('Telegram', 'username')
    db_name = config.get('Telegram', 'db_name')

    # Initialize SQLite
    import sqlite3
    con = sqlite3.connect("channels.sqlite")
    # con.close()
    scraper.create_db(con)

    # create the client and connect
    client = TelegramClient(username, api_id, api_hash)

    # start the client
    await client.start()
    print("------------------- Client Created -----------------------")

    # ensure you're authorized
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            print("Your account has two-step verification enabled. Please enter your password.")
            await client.sign_in(password=input('Password: '))
    return con, client

async def main():
    import traceback
    con, client = await config_session()
    ch_list = [channel_list[0]]
    # TEMP
    con = sqlite3.connect('channels.sqlite')
    scraper.create_db(con)
    # END TEMP
    try:
        for usr in ch_list:
            channel_info = await scraper.get_channel(usr, client)
            scraper.save_channel_to_db(channel_info, con) 
            my_channel = await client.get_entity(usr)
            for period in periods:
                print("----------------------------------------------------------")
                messages = await scraper.scrape_messages(period, client, my_channel, msg_id)
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
    
#!/usr/bin/env python
# coding: utf-8
from telescraper import scraper
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import datetime
import sqlite3

# decided periods to scrape    
periods = [(datetime.datetime(2023, 2, 23), datetime.datetime(2023, 2, 24))]

channel_list = ["https://t.me/activatica"]

async def config_session(inifile="config.ini"):
    config = scraper.configparser.ConfigParser()
    config.read(inifile)

    # setting configuration values
    api_id = config.get('Telegram', 'api_id')
    api_hash = config.get('Telegram', 'api_hash')
    phone = config.get('Telegram', 'phone')
    username = config.get('Telegram', 'username')

    # Initialize SQLite
    import sqlite3
    con = sqlite3.connect("channels.db")
    # con.close()
    scraper.create_db(con)

    # create the client and connect
    client = TelegramClient(username, api_id, api_hash)

    # start the client
    await client.start()
    print("Client Created")

    # ensure you're authorized
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            print("Your account has two-step verification enabled. Please enter your password.")
            await client.sign_in(password=input('Password: '))
    return [con, client]

async def main():
    [con, client] = config_session()
    ch_list = [channel_list[0]]
    # TEMP
    con = sqlite3.connect("channels.db")
    scraper.create_db(con)
    # END TEMP
    try:
        for usr in ch_list:
            channel_info = await scraper.get_channel(usr, client)
            scraper.save_channel_to_db(channel_info, con) 
            my_channel = await client.get_entity(usr)
            print("Done with channel info")
            for period in periods:
                print("start of messages")
                messages = await scraper.scrape_messages(period, client, my_channel)
                scraper.save_messages_to_db(messages, con) 

            con.commit()
            break
            
    except Exception as e:
        print(e)
    finally:        
        con.close()


if __name__ == "__main__":
  main()
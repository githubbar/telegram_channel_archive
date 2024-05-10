#!/usr/bin/env python
# coding: utf-8

import configparser
import json
import asyncio
import datetime
from dateutil.relativedelta import relativedelta
from telethon.errors import SessionPasswordNeededError

import os, sys
import numpy as np
import time
import pytz
import re, locale
from IPython.display import Image, display
from telethon import TelegramClient
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel, PeerUser
)
from concurrent.futures import CancelledError

import telethon.tl.types as media_types

import os
from collections import defaultdict
import sqlite3

# some functions to parse json date correctly
async def config_session(inifile="config.ini", sessionfile=None):
    config = configparser.ConfigParser()
    config.read(inifile)

    # setting configuration values
    api_id = config.get('Telegram', 'api_id')
    api_hash = config.get('Telegram', 'api_hash')
    phone = config.get('Telegram', 'phone')
    username = config.get('Telegram', 'username')

    print(username, api_id, api_hash)
    # create the client and connect
    if sessionfile:
        client = TelegramClient(sessionfile, api_id, api_hash)
    else:
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
    return client

def config_db(inifile="config.ini"):
    config = configparser.ConfigParser()
    config.read(inifile)
    db_name = config.get('Telegram', 'db_name')

    # Initialize SQLite
    con = sqlite3.connect(db_name, autocommit=False)
    return con

def create_db(inifile="config.ini"):
    con = sqlite3.connect('db.sqlite')
    with open('create_db.sql', 'r') as sql_file:
        sql_script = sql_file.read()
        con.executescript(sql_script)
    return con

async def get_comments(message, client, my_channel):
    replies = []
    comment_count = 0
    comment_groups = {}
    
    #find the replies in chat
    channel_full_info = await client(GetFullChannelRequest(channel=my_channel))
    chat = channel_full_info.chats[1].id  # <----- replies are stored in chat
    chat_entity = PeerChannel(int(chat))
    my_chat = await client.get_entity(chat_entity)

    # process replies
    async for comment in client.iter_messages(my_channel, reverse=True, reply_to = message.id, limit = 25):
        comment_obj = {}
        comment_obj['id'] = comment.id
        comment_obj['channel_id'] = my_channel.id    
        comment_obj['date'] = comment.date
        comment_obj['text'] = comment.text
        comment_obj['message_id'] = message.id       
        comment_obj['from_user_id'], comment_obj['from_channel_id'], comment_obj['channel_name'], comment_obj['channel_username'] =  None, None, None, None
        comment_obj['reactions'], comment_obj['media'], comment_obj['fwd_title'], comment_obj['fwd_channel_id'], comment_obj['fwd_username'] = None, None, None, None, None
        # if the message is part of the group keep track of ID that belong to the group
        if comment.grouped_id:
            comment_groups.setdefault(comment.grouped_id, []).append(comment.id)
            print("Comment Group:", comment_groups)
            if comment.text == "":
                #skip the message as we will go through it when downloading media
                continue

        if hasattr(comment.from_id, 'user_id'):
            comment_obj['from_user_id'] = comment.from_id.user_id
        elif hasattr(comment.from_id, 'channel_id'):
            comment_obj['from_channel_id'] = comment.from_id.channel_id
            try:
                comment_channel = await client.get_entity(PeerChannel(int(comment.from_id.channel_id)))
                comment_channel_info = await client(GetFullChannelRequest(channel=comment_channel))
                comment_obj['channel_name'] = comment_channel_info.chats[0].title
                comment_obj['channel_username'] = comment_channel_info.chats[0].username
            except:
                pass
        if comment.reactions:
            comment_obj['reactions'] = [(reaction.reaction.emoticon, reaction.count) for reaction in comment.reactions.results if hasattr(reaction.reaction, 'emoticon')]
       
        if comment.fwd_from:
            try:
                fwd_from_channel = await client.get_entity(PeerChannel(int(comment.fwd_from.from_id.channel_id)))
                fwd_channel_info = await client(GetFullChannelRequest(channel=fwd_from_channel))
                comment_obj['fwd_title'] = fwd_channel_info.chats[0].title
                comment_obj['fwd_username'] = fwd_channel_info.chats[0].username
            except:
                if comment.fwd_from.from_id:
                    comment_obj['fwd_channel_id'] = int(comment.fwd_from.from_id.channel_id)
                if comment.fwd_from.from_name:
                    comment_obj['fwd_title'] = comment.fwd_from.from_name
    
        # Media in comments naminng is photo[message_id]_comment[comment_number]
        if comment.media:
            comment_obj['media'] = await get_media(comment, client, my_chat, comment_groups, is_group = False, group_main_id = None, is_comment = True, comment_media_id = str(message.id)+"_comment"+str(comment_count))

        if comment_obj:
            replies.append(comment_obj)

    return replies

async def get_media(message, client, my_channel, groups, is_group, group_main_id, is_comment = False, comment_media_id = None):
    # print("--------- Processing media ---------")
    media = [{}]
    if message.media:
#         print("Has media")
        if message.photo or (hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "image"):
            media[0] = await process_image(message, client, my_channel, groups, is_group, group_main_id, is_comment = is_comment, comment_media_id=comment_media_id)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "video":
            media[0] = process_video(message)
        elif hasattr(message.media, 'webpage'):
            if hasattr(message.media.webpage, 'url'):
                media[0] = await process_webpage(message)
        elif hasattr(message.media, 'poll'):
            media[0] = process_poll(message)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "audio":
            media[0] = process_audio(message)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "application":
            media[0] = await process_document(message, client, groups, is_group, group_main_id, is_comment = is_comment, comment_media_id = comment_media_id)
        else:
            print(message.media)
            print('message id', message.id)
            
        if message.grouped_id:
            media.extend(await process_media(message, client, my_channel, groups, is_group, group_main_id, is_comment, comment_media_id))
    
    return media

async def process_media(message, client, my_channel, groups, is_group, group_main_id, is_comment=False, comment_media_id = None):
    print("Group message")
    media = []
    media_counter = 1
    async for m in client.iter_messages(my_channel, ids = groups[message.grouped_id][::-1]):
        media_obj = {}
        if m.id != message.id:
            if m.photo or (hasattr(m.media, 'document') and str(m.media.document.mime_type).split('/', 1)[0] == "image"):
                media_obj = await process_image(m, client, my_channel, groups, is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id=comment_media_id)
            elif hasattr(m.media, 'document') and str(m.media.document.mime_type).split('/', 1)[0] == "video":
                media_obj = process_video(m)
            elif hasattr(m.media, 'webpage'):
                if hasattr(m.media.webpage, 'url'):
                    media_obj = await process_webpage(m)
            elif hasattr(m.media, 'poll'):
                media_obj = process_poll(m)
            elif hasattr(m.media, 'document') and str(m.media.document.mime_type).split('/', 1)[0] == "audio":
                media_obj = process_audio(m)
            elif hasattr(m.media, 'document') and str(m.media.document.mime_type).split('/', 1)[0] == "application":
                media_obj = await process_document(m, client, groups, is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id = comment_media_id)
            
            if media_obj:
                media.append(media_obj)
            media_counter+=1
    return media

async def process_webpage(message):
#     print("Photo: True")
    media_obj = {}
    media_obj['type'] = 'webpage'
    media_obj['file_name'] = message.media.webpage.url
    return media_obj

async def process_image(message, client, my_channel, groups, is_group, group_main_id, media_counter = 0, is_comment=False, comment_media_id = None):
#     print("Photo: True")
    media_obj = {}
    media_obj['type'] = "photo"
    folder = './temp'
    folder_path = str(folder+"/"+"images/") # this will be in images folder of the channel
    media_obj['group_main_id'] = group_main_id
    media_obj['file_name'] = await download_media(message, folder_path, client, groups, "photo", is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id = comment_media_id)
    return media_obj

def process_video(message):
    # print("Video: True")
    media_obj = {}
    media_obj['type'] = "video"
    media_obj['video_type'] = str(message.media.document.mime_type).split('/', 1)[1]
    media_obj['video_size'] = message.media.document.size
    if hasattr(message.media.document.attributes[0], 'duration'):
        media_obj['video_duration'] = message.media.document.attributes[0].duration
    try:
        media_obj['file_name'] = message.media.document.attributes[1].file_name
    except:
        media_obj['file_name'] = ''
    return media_obj

def process_poll(message):
#     print("Poll: True")
    media_obj = {}
    media_obj['type'] = "poll"
    media_obj['question'] = message.media.poll.question
    media_obj['answers'] = [(answer.text, answer.option) for answer in message.media.poll.answers]
    if message.media.poll.close_date:
        media_obj['close date'] = message.media.poll.close_date
    media_obj['total voters'] = message.media.results.total_voters
    media_obj['file_name'] = None
    return media_obj

def process_audio(message):
#     print("Audio: True")
    media_obj = {}
    media_obj['type'] = "audio"
    media_obj['audio_size'] = message.media.document.size
    if hasattr(message.media.document.attributes[0], 'duration'):
        media_obj['audio_duration'] = message.media.document.attributes[0].duration
    try:
        media_obj['file_name'] = message.media.document.attributes[1].file_name
    except:
        media_obj['file_name'] = ''
    return media_obj

async def process_document(message, client, groups, is_group, group_main_id, media_counter = 0, is_comment=False, comment_media_id = None):
    # print("Doc: True")
    media_obj = {}
    media_obj['type'] = "document"
    media_obj['document_size'] = message.media.document.size
    try:
        media_obj['file_name'] = message.media.document.attributes[1].file_name
    except:
        media_obj['file_name'] = ''
    folder = './temp'
    folder_path = str(folder+"/"+"documents/")
    media_obj['file_name'] = await download_media(message, folder_path, client, groups, "doc", is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id = comment_media_id)
    return media_obj

async def download_media(message, folder_path, client, groups, file_type, is_group, group_main_id, media_counter = 0, is_comment=False, comment_media_id = None):
#     print("Downloading media")
    wait_time = 1
    max_retries=10 # this should be figured out as it might still timeout
    for attempt in range(max_retries):
        try:
            path = await client.download_media(message.media, folder_path) # this will be in images folder of the channel
            if is_comment:
                if media_counter >0:
                    renamed_path = folder_path+file_type+comment_media_id +str("_") +str(media_counter) + str(".")+str(path.rsplit('.', 1)[-1])
                else:
                    renamed_path = folder_path+file_type+comment_media_id + str(".")+str(path.rsplit('.', 1)[-1])
            elif is_group:
                if media_counter >0:
                    renamed_path = folder_path+file_type+ str(group_main_id)+str("_") +str(media_counter) + str(".")+str(path.rsplit('.', 1)[-1])
                else:
                    renamed_path = folder_path+file_type+ str(group_main_id) + str(".")+str(path.rsplit('.', 1)[-1])
            else:
                renamed_path = folder_path+file_type+ str(message.id) + str(".")+str(path.rsplit('.', 1)[-1])

            os.rename(path, renamed_path)
            return renamed_path
        except Exception as e:
            print(f"Caught an exception of type {type(e)}: {str(e)}")
            time.sleep(wait_time)
            wait_time *= 2 # Exponential backoff
    print(f"Failed to download file after {max_retries} attempts. {message.media}")

async def get_channel(client, usr):
    """
    Fetches detailed information about a Telegram channel or chat based on the provided `usr` identifier (either a numeric ID or username) using a given `client` connection. 
    This function:
    1. Identifies the specific channel.
    2. Retrieves details about it.
    3. Constructs a dictionary with this information.
    4. Creates a sanitized folder named after the channel's username.
    5. Downloads and stores the channel's profile photo in the aforementioned folder.
    6. Serializes and saves the constructed information in a JSON file inside the folder.
    7. Returns the name of the created folder.

    Parameters:
    - usr: Either a numeric channel ID or a string username.
    - client: A connection client to the Telegram API.

    Returns:
    - The name of the directory where the channel's information and profile photo are stored.
    """


    if usr.isdigit():
        entity = PeerChannel(int(usr))
    else:
        entity = usr

    my_channel = await client.get_entity(entity)
    channel_full_info = await client(GetFullChannelRequest(channel=my_channel))
    chat_count = len(channel_full_info.chats)
    # print("--------- Printing channel information ---------")


    # General channel information
    channel_info = {
        'id': channel_full_info.chats[0].id,
        'title': channel_full_info.chats[0].title,
        'username': channel_full_info.chats[0].username,
        'description': channel_full_info.full_chat.about,
        'total_particiant_time': datetime.datetime.now(),
        'total_participants': channel_full_info.full_chat.participants_count,
        'participants': my_channel.usernames,
    }
    for attr, value in channel_full_info.chats[0].__dict__.items():
        if isinstance(value, bool):
            channel_info[attr] = value
    print(" Title: ", channel_full_info.chats[0].title)
    print(" username: ", channel_full_info.chats[0].username)

    if chat_count > 1: # if more than 1, then the channel has a chat
        print(" chat:", True)
        channel_info.update({
            'chat': True,
            'chat_title': channel_full_info.chats[1].title,
            'chat_id': channel_full_info.chats[1].id,
        })
        l = []
    
        for attr, value in channel_full_info.chats[1].__dict__.items():
            if isinstance(value, bool):
                channel_info[f'chat_{attr}'] = value    
    return channel_info

async def get_message_dict(message, client, my_channel, groups, is_group = False, group_main_id = None):
    current_message = {}
    current_message['id'] = message.id
    current_message['channel_id'] = my_channel.id
    current_message['date'] = message.date
    # print("Date:", message.date)
    current_message['post_author'] = message.post_author if message.post_author else "channel"
    current_message['text'] = message.text
    current_message['mentions'] = message.mentioned
    current_message['total_views'] = message.views
    current_message['total_fwds'] = message.forwards
    # ADDITIONAL FIELDS
    current_message['hidden_edit'] = message.edit_hide
    current_message['last_edit_date'] = message.edit_date
    current_message['scheduled'] = message.from_scheduled
    current_message['via_bot_id'] = message.via_bot_id
    current_message['noforwards'] = message.noforwards
    current_message['ttl_period'] = message.ttl_period
  
    if message.reactions:
        current_message['reactions'] = [(reaction.reaction.emoticon, reaction.count) for reaction in message.reactions.results if hasattr(reaction.reaction, 'emoticon')]
    current_message['fwd_title'], current_message['fwd_channel_id'], current_message['fwd_username'] = None, None, None
    if message.fwd_from:
        try:
            fwd_from_channel = await client.get_entity(PeerChannel(int(message.fwd_from.from_id.channel_id)))
            fwd_channel_info = await client(GetFullChannelRequest(channel=fwd_from_channel))
            current_message['fwd_title'] = fwd_channel_info.chats[0].title
            current_message['fwd_username'] = fwd_channel_info.chats[0].username
        except:
            if message.fwd_from.from_id:
                current_message['fwd_channel_id'] = int(message.fwd_from.from_id.channel_id)
            if message.fwd_from.from_name:
                current_message['fwd_title'] = message.fwd_from.from_name
    
    current_message['media'] = await get_media(message, client, my_channel, groups, is_group, group_main_id)
    
    return current_message

async def scrape_messages(client, my_channel, period=(None, None), ids=None):
    messages = []
    groups = {}
    # Handle time periods or no time periods
    if period[0]  and period[1]:
        dt1, dt2 = map(lambda dt: dt.replace(tzinfo=datetime.timezone.utc), period)
    else:
        dt1 = dt2 = None

    async for message in client.iter_messages(my_channel, offset_date = dt2, ids=ids):
        if (dt1 and dt2) and dt1 > message.date: # skip if not in date range, but keep all the comments
            break 

        # if the message is part of the group keep track of IDs that belong to the group
        if message.grouped_id:
            groups.setdefault(message.grouped_id, []).append(message.id)
            #skip the message as we will go through it when downloading media
            continue
        
        print(f'ID: {message.id} date: {message.date:%m/%d/%Y, %H:%M:%S}')        
        

        # [print(f'    {c}') for c in comments]
        # print("----------------------------------------------------------")        
        current_message = await get_message_dict(message, client, my_channel, groups)
        current_message['comments'] = await get_comments(message, client, my_channel) if message.replies and message.replies.replies else None
        messages.append(current_message)

    # Coming back to the grouped messages
    for group in groups:
        # Iterate over messages for the current group
        async for message in client.iter_messages(my_channel, ids = groups[group][::-1]):
            if message.text == "":
                # if not -> concat the texts of all
                # print("ID:", message.id)
                # print("Group:", message.grouped_id)
                continue
            else: 
                pass
                print(f'Group ID: {message.id} date: {message.date:%m/%d/%Y, %H:%M:%S}')        
            current_message = await get_message_dict(message, client, my_channel, groups, is_group= True, group_main_id = message.id)
            current_message['comments'] = await get_comments(message, client, my_channel) if message.replies and message.replies.replies else None
            # current_message['comments'] = None
            # If reactions are missing, iterate to find them
            if 'reactions' not in current_message.keys():
                async for message in client.iter_messages(my_channel, ids = groups[group][::-1]):
                    if message.reactions:
                        current_message['reactions'] = [(reaction.reaction.emoticon, reaction.count) for reaction in message.reactions.results if hasattr(reaction.reaction, 'emoticon')]
                        break
            messages.append(current_message)
    return messages

def save_channel_to_db(channel_info, con):    
    """ Save Channel fields to DB """
    cur = con.cursor()
    columns = ', '.join(channel_info.keys())
    placeholders = ', '.join('?' * len(channel_info))
    sql = 'INSERT INTO channel ({}) VALUES ({})'.format(columns, placeholders)
    values = [str(x).replace('\'', '') if isinstance(x, list) else x for x in channel_info.values()]
    # print(sql)
    # print(values)
    try:
        cur.execute(sql, values)
        con.commit()
    except sqlite3.IntegrityError as e:
        print(f'INFO: Channel title={channel_info['title']} already in DB.')
        con.rollback()

def save_messages_to_db(messages, con):    
    """ Save Message fields to DB """
    cur = con.cursor()
    exclude_fields = {'media', 'comments'}
    for msg in messages:
        msg_col_names = [key for key in msg.keys() if key not in exclude_fields]
        msg_col_values = [msg[key] for key in msg_col_names] 
        columns = ', '.join(msg_col_names)
        placeholders = ', '.join('?' * len(msg_col_names))
        sql = 'INSERT INTO message ({}) VALUES ({})'.format(columns, placeholders)
        values = [str(x).replace('\'', '') if isinstance(x, list) else x for x in msg_col_values]
        try:
            cur.execute(sql, values)
            save_media_to_db(msg['media'], msg, None, con) 
            save_comments_to_db(msg['comments'], msg, con) 
            con.commit()
        except sqlite3.IntegrityError as e:
            print(f'INFO: Message id={msg['id']} already in DB.')
            con.rollback()

def save_media_to_db(media, msg, comment_id, con):    
    """ Save media blobs to DB """
    cur = con.cursor()
    for m in media:    
        if not len(m):
            continue
        # print(m)
        if m['type'] in {'photo', 'document'}:
            data = convertToBinaryData(m['file_name'])
            file_name = os.path.basename(m['file_name'])            
        else:
            data =  None
            file_name = m['file_name']            
        sql = 'INSERT INTO media (channel_id, message_id, comment_id, type, file_name, media) VALUES (?, ?, ?, ?, ?, ?)'
        values = (msg['channel_id'], msg['id'], comment_id, m['type'], file_name, data)
        # print(sql)
        try:
            cur.execute(sql, values)
        except sqlite3.IntegrityError as e:
            print(f'INFO: Media for message id={msg['id']} already in DB.')

def save_comments_to_db(comments, msg, con):    
    if not comments: 
        return
    """ Save comments blobs to DB """
    cur = con.cursor()
    exclude_fields = {'media'}
    for com in comments:    
        com_col_names = [key for key in com.keys() if key not in exclude_fields]
        com_col_values = [com[key] for key in com_col_names] 
        columns = ', '.join(com_col_names)
        placeholders = ', '.join('?' * len(com_col_names))        
        sql = 'INSERT INTO comment ({}) VALUES ({})'.format(columns, placeholders)
        values = [str(x).replace('\'', '') if isinstance(x, list) else x for x in com_col_values]
        # print(sql)
        try:        
        # IF this comment has media
            cur.execute(sql, values)
            if com['media']:
                save_media_to_db(com['media'], msg, cur.lastrowid, con) 
        except sqlite3.IntegrityError as e:
            print(f'INFO: Comment for message id={msg['id']} already in DB.')
        except sqlite3.OperationalError as e:
            print(f'Error: {e} \n values = {values} \n sql = {sql} ')

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData




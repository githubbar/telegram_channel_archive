#!/usr/bin/env python
# coding: utf-8

import configparser
import json
import asyncio
import datetime
from dateutil.relativedelta import relativedelta

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

# some functions to parse json date correctly
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)

# class Scraper:
# Create DB tables
def create_db(con):
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS channel")
    cur.execute("DROP TABLE IF EXISTS message")
    cur.execute("DROP TABLE IF EXISTS media")    

    cur.execute("""CREATE TABLE channel(
    id INTEGER PRIMARY KEY,
    title, username, description, total_particiant_time, total_participants, participants, 
    creator,left,broadcast,verified,megagroup,restricted,signatures,min,scam,has_link,has_geo,slowmode_enabled,call_active,call_not_empty,fake,gigagroup,noforwards,join_to_send,join_request,forum,stories_hidden,stories_hidden_min,stories_unavailable,
    chat, chat_title, chat_id,
    chat_creator,chat_left,chat_broadcast,chat_verified,chat_megagroup,chat_restricted,chat_signatures,chat_min,chat_scam,chat_has_link,chat_has_geo,chat_slowmode_enabled,chat_call_active,chat_call_not_empty,chat_fake,chat_gigagroup,chat_noforwards,chat_join_to_send,chat_join_request,chat_forum,chat_stories_hidden,chat_stories_hidden_min,chat_stories_unavailable
    )""")
    cur.execute("""CREATE TABLE message(
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    date, post_author, text, mentions, total_views, total_fwds, hidden_edit, last_edit_date, scheduled, via_bot_id, noforwards, ttl_period, total_replies, replies, reactions,
    fwd_title, fwd_username, fwd_channel_id
    )""")
    cur.execute("""CREATE TABLE media(
    id INTEGER PRIMARY KEY,
    message_id INTEGER NOT NULL,
    media BLOB
    )""")

async def get_replies(message, client, my_channel):
    replies = []
    # TEMP: 
    return replies
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
        # if the message is part of the group keep track of ID that belong to the group
        if comment.grouped_id:
            comment_groups.setdefault(comment.grouped_id, []).append(comment.id)
            print("Comment Group:", comment_groups)
            if comment.text == "":
                #skip the message as we will go through it when downloading media
                continue
        if hasattr(comment.from_id, 'user_id'):
            comment_obj['user id'] = comment.from_id.user_id
        elif hasattr(comment.from_id, 'channel_id'):
            comment_obj['channel id'] = comment.from_id.channel_id
            try:
                comment_channel = await client.get_entity(PeerChannel(int(comment.from_id.channel_id)))
                comment_channel_info = await client(GetFullChannelRequest(channel=comment_channel))
                comment_obj['channel name'] = comment_channel_info.chats[0].title
                comment_obj['channel username'] = comment_channel_info.chats[0].username
            except:
                pass

        comment_obj['comment date'] = comment.date
        comment_obj['comment text'] = comment.message


        if comment.reactions:
            comment_obj['reactions'] = [(reaction.reaction.emoticon, reaction.count) for reaction in comment.reactions.results if hasattr(reaction.reaction, 'emoticon')]
        
        if comment.fwd_from:
            comment_obj['fwd'] = {}
            try:
                fwd_from_channel = await client.get_entity(PeerChannel(int(comment.fwd_from.from_id.channel_id)))
                fwd_channel_info = await client(GetFullChannelRequest(channel=fwd_from_channel))
                comment_obj['fwd']['fwd_title'] = fwd_channel_info.chats[0].title
                comment_obj['fwd']['fwd_username'] = fwd_channel_info.chats[0].username
            except:
                if comment.fwd_from.from_id:
                    comment_obj['fwd']['channel_id'] = int(comment.fwd_from.from_id.channel_id)
                if comment.fwd_from.from_name:
                    comment_obj['fwd']['fwd_title'] = comment.fwd_from.from_name
        
        
        
        # Media in comments naminng is photo[message_id]_comment[comment_number]
        if comment.media:
            comment_obj['media'] = await get_media(comment, client, my_chat, comment_groups, is_group = False, group_main_id = None, is_comment = True, comment_media_id = str(message.id)+"_comment"+str(comment_count))


        if comment_obj:
                replies.append(comment_obj)

        comment_count+=1
    
    return replies

async def get_media(message, client, my_channel, groups, is_group, group_main_id, is_comment = False, comment_media_id = None):
    print("Processing media")
    media = [{}]
    if message.media:
#         print("Has media")
        if message.photo or (hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "image"):
            media[0] = await process_image(message, client, my_channel, groups, is_group, group_main_id, is_comment = is_comment, comment_media_id=comment_media_id)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "video":
            media[0] = process_video(message)
        elif hasattr(message.media, 'webpage'):
            if hasattr(message.media.webpage, 'url'):
                media[0]['url'] = message.media.webpage.url
        elif hasattr(message.media, 'poll'):
            media[0] = process_poll(message)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "audio":
            media[0] = process_audio(message)
        elif hasattr(message.media, 'document') and str(message.media.document.mime_type).split('/', 1)[0] == "application":
            media[0] = await process_document(message, client, groups, is_group, group_main_id, is_comment = is_comment, comment_media_id = comment_media_id)
        else:
            print(message.media)
            print('message id', message.id)
            
        # TODO: grouped images? does this work?
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
                    media_obj['url'] = m.media.webpage.url
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

async def process_image(message, client, my_channel, groups, is_group, group_main_id, media_counter = 0, is_comment=False, comment_media_id = None):
#     print("Photo: True")
    media_obj = {}
    media_obj['type'] = "photo"
    folder = './temp'
    folder_path = str(folder+"/"+"images/") # this will be in images folder of the channel
    media_obj['group_main_id'] = group_main_id
    media_obj['image_path'] = await download_media(message, folder_path, client, groups, "photo", is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id = comment_media_id)
    return media_obj

def process_video(message):
#     print("Video: True")
    media_obj = {}
    media_obj['type'] = "video"
    media_obj['video_type'] = str(message.media.document.mime_type).split('/', 1)[1]
    media_obj['video_size'] = message.media.document.size
    if hasattr(message.media.document.attributes[0], 'duration'):
        media_obj['video_duration'] = message.media.document.attributes[0].duration
    try:
        media_obj['file_name'] = message.media.document.attributes[1].file_name
    except:
        media_obj['file_name'] = None
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
        media_obj['file_name'] = None
    return media_obj

async def process_document(message, client, groups, is_group, group_main_id, media_counter = 0, is_comment=False, comment_media_id = None):
#     print("Doc: True")
    media_obj = {}
    media_obj['type'] = "document"
    media_obj['document_size'] = message.media.document.size
    try:
        media_obj['file_name'] = message.media.document.attributes[1].file_name
    except:
        media_obj['file_name'] = None
    folder = './temp'
    folder_path = str(folder+"/"+"documents/")
    media_obj['document_path'] = await download_media(message, folder_path, client, groups, "doc", is_group, group_main_id, media_counter = media_counter, is_comment = is_comment, comment_media_id = comment_media_id)
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

async def get_channel(usr, client):
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
    print("Printing channel information")


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
    print("Title: ", channel_full_info.chats[0].title)
    print("username: ", channel_full_info.chats[0].username)

    if chat_count > 1: # if more than 1, then the channel has a chat
        print("chat:", True)
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

def save_channel_to_db(channel_info, con):    
    """ Save Channel fields to DB """
    cur = con.cursor()
    columns = ', '.join(channel_info.keys())
    placeholders = ', '.join('?' * len(channel_info))
    sql = 'INSERT INTO channel ({}) VALUES ({})'.format(columns, placeholders)
    values = [str(x).replace('\'', '') if isinstance(x, list) else x for x in channel_info.values()]
    # print(sql)
    # print(values)
    cur.execute(sql, values)

async def get_message_dict(message, client, my_channel, groups, is_group = False, group_main_id = None):
    current_message = {}
    current_message['id'] = message.id
    current_message['channel_id'] = my_channel.id
    current_message['date'] = message.date
    print("Date:", message.date)
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
    if message.replies and message.replies.replies != 0:
        current_message['total_replies'] = message.replies.replies
        current_message['replies'] = await get_replies(message, client, my_channel)
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

async def scrape_messages(period, client, my_channel):
    start_time = datetime.datetime.now()
    messages = {}
    count = 0
    groups = {}

    dt1, dt2 = map(lambda dt: dt.replace(tzinfo=datetime.timezone.utc), period)
    print("")
    print("starting:", dt1, dt2)

    async for message in client.iter_messages(my_channel, offset_date = dt2):
        if dt1 > message.date:
            break

        print("ID:", message.id)
        
        # if the message is part of the group keep track of IDs that belong to the group
        if message.grouped_id:
            groups.setdefault(message.grouped_id, []).append(message.id)
            #skip the message as we will go through them separately
            continue
                
        current_message = await get_message_dict(message, client, my_channel, groups)
        messages[count] = current_message
        print("")
        count += 1
        # # TEMP
        # return messages 
        # # END TEMP

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
                print("ID:", message.id)
                print("Group:", message.grouped_id)
            current_message = await get_message_dict(message, client, my_channel, groups, is_group= True, group_main_id = message.id)
        # If reactions are missing, iterate to find them
        if 'reactions' not in current_message.keys():
            async for message in client.iter_messages(my_channel, ids = groups[group][::-1]):
                if message.reactions:
                    current_message['reactions'] = [(reaction.reaction.emoticon, reaction.count) for reaction in message.reactions.results if hasattr(reaction.reaction, 'emoticon')]
                    break
        # If replies are missing, iterate to find them
        if 'replies' not in current_message.keys():
            async for message in client.iter_messages(my_channel, ids = groups[group][::-1]):
                if message.replies and message.replies.replies != 0:
                    current_message['total replies'] = message.replies.replies
                    current_message['replies'] = await get_replies(message, client, my_channel)
                    break


        messages[count] = current_message
        print("")
        count += 1
    return messages

def save_messages_to_db(messages, channel_id, con):    
    """ Save Message fields to DB """
    cur = con.cursor()
    for msg in messages.items():
        msg_col_names = [key for key in msg[1].keys() if key != 'media']
        # add channel_id to all the column names
        msg_col_values = [msg[1][key] for key in msg_col_names] 
        columns = ', '.join(msg_col_names)
        placeholders = ', '.join('?' * len(msg_col_names))
        sql = 'INSERT INTO message ({}) VALUES ({})'.format(columns, placeholders)
        values = [str(x).replace('\'', '') if isinstance(x, list) else x for x in msg_col_values]
        cur.execute(sql, values)
        save_media_to_db(msg[1]['media'], cur.lastrowid, con)        

def save_media_to_db(media, msg_id, con):    
    """ Save media blobs to DB """
    cur = con.cursor()
    for m in media:    
        print(m)
        data = convertToBinaryData(m['image_path'])
        sql = 'INSERT INTO media (message_id, media) VALUES (?, ?)'
        values = (msg_id, data)
        # print(sql)
        cur.execute(sql, values)


def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData




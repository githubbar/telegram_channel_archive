DROP TABLE IF EXISTS channel;
DROP TABLE IF EXISTS message;
DROP TABLE IF EXISTS comment;
DROP TABLE IF EXISTS media; 

CREATE TABLE channel(
    id INTEGER PRIMARY KEY,
    title, username, description, total_particiant_time, total_participants, participants, 
    creator,left,broadcast,verified,megagroup,restricted,signatures,min,scam,has_link,has_geo,slowmode_enabled,call_active,call_not_empty,fake,gigagroup,noforwards,join_to_send,join_request,forum,stories_hidden,stories_hidden_min,stories_unavailable,
    chat, chat_title, chat_id,
    chat_creator,chat_left,chat_broadcast,chat_verified,chat_megagroup,chat_restricted,chat_signatures,chat_min,chat_scam,chat_has_link,chat_has_geo,chat_slowmode_enabled,chat_call_active,chat_call_not_empty,chat_fake,chat_gigagroup,chat_noforwards,chat_join_to_send,chat_join_request,chat_forum,chat_stories_hidden,chat_stories_hidden_min,chat_stories_unavailable
    );

CREATE TABLE message(
    id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    date, post_author, text, mentions, total_views, total_fwds, hidden_edit, last_edit_date, scheduled, via_bot_id, noforwards, ttl_period, reactions,
    fwd_title, fwd_username, fwd_channel_id,
    PRIMARY KEY (id, channel_id)
    );

CREATE TABLE comment(
    id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    date, text, reactions, 
    from_user_id INTEGER,
    from_channel_id INTEGER,
    channel_name, channel_username, 
    fwd_title, fwd_username, fwd_channel_id, 
    PRIMARY KEY (id, channel_id, message_id)
    );

CREATE TABLE media(
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    comment_id INTEGER,
    type, 
    file_name,
    media BLOB
    );

-- CREATE TABLE message2(
--     id INTEGER NOT NULL,
--     channel_id INTEGER NOT NULL,
--     date, post_author, text, mentions, total_views, total_fwds, hidden_edit, last_edit_date, scheduled, via_bot_id, noforwards, ttl_period, reactions,
--     fwd_title, fwd_username, fwd_channel_id,
--     PRIMARY KEY (id, channel_id)
--     );
-- INSERT INTO message2 SELECT * FROM message;
-- 	
-- CREATE TABLE comment2(
--     id INTEGER NOT NULL,
--     channel_id INTEGER NOT NULL,
--     message_id INTEGER NOT NULL,
--     date, text, reactions, 
--     from_user_id INTEGER,
--     from_channel_id INTEGER,
--     channel_name, channel_username, 
--     fwd_title, fwd_username, fwd_channel_id, 
--     PRIMARY KEY (id, channel_id, message_id)
--     );
-- INSERT INTO comment2 SELECT * FROM comment;	
-- DROP TABLE message;
-- DROP TABLE comment;
-- ALTER TABLE `comment2` RENAME TO `comment`;
-- ALTER TABLE `message2` RENAME TO `message`;
-- VACUUM;
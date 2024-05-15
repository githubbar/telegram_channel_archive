- Messages with all fields NULL correspond to special events:
	- Channel Created, Channel Photo Updated, Pinned Message
	identifiable by: WHERE message.text=NULL

- Messages with text field empty (but not NULL)
	- These are just posts without text. Mostly images.

- Messages with content forwarded from other channels:
	- Save only channel id if the channel forwarded from is not public.
	- Otherwise save forward channel title and username

- Is there a way to get time of post/channel in local timezone?
	- looks like no, it is all stores as a unix epoch w/o the zone info. See: "the time in Telegram is based on the timezone settings of your phone" https://twitter.com/telegram/status/837174425678188544?lang=en
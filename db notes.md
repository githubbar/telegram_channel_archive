- Messages with all fields NULL correspond to special events:
	- Channel Created, Channel Photo Updated, Pinned Message
	identifiable by: WHERE message.text=NULL


- Messages with text filed empty (but not NULL)
	- These are just posts without text. Mostly images.

- Messages with content forwarded from other channels:
	- Save only channel id if the channel forwarded from is not public.
	- Otherwise save forward channel title and username

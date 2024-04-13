# telegram_channel_archive
Telegram scraping for archiving the channels, messages and related content such as images, files and comments into sqlite DB.

# Install and run:
1) download and unzip the project or 'git clone https://github.com/githubbar/telegram_channel_archive'
2) create conda environment and install required packages 'conda create --name tele --file requirements.txt'
3) activate 'conda activate tele'
4) create config.ini file and put your telegram settings there (see below)
3) run 'python main.py'

# Notes
requirements is created via: conda list -e > requirements.txt

# config.ini will look like this
[Telegram]
api_id = 
api_hash = 
phone = 
username = 
db_name = 

# Unit tests
Run tests.py which loads and runs all unit tests from ./tests subdir

Current tests:
    test_grouped_media - make sure the correct number of grouped media are downloaded
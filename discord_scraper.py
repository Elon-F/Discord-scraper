import logging
import os
import time
from asyncio import sleep
from sys import stdout
from typing import List

import selfcord
from selfcord import Message
from selfcord.utils import _ColourFormatter

from data import DataManager

token = os.getenv("DISCORD_TOKEN")
is_bot = bool(os.getenv("DISCORD_BOT"))
fetch_limit = int(os.getenv("MESSAGE_FETCH_LIMIT", 500))
target_channel_ids = [int(x) for x in os.getenv("TARGET_CHANNELS").split(',') if x]

log_level = int(os.getenv("LOG_LEVEL", logging.INFO))

client = selfcord.Client()

scraped_initiated = False


@client.event
async def on_ready():
    global scraped_initiated
    logging.info(f'Logged on as {client.user}!')
    if not scraped_initiated:
        scraped_initiated = True
        scraper = Scraper(client, DataManager(target_channel_ids), message_fetch_limit=fetch_limit)
        await scraper.begin_scraping()


def delay_to_next_minute():
    return 60 - time.time() % 60


def delay_to_next_hour():
    return (3600 - time.time()) % 3600


def delay_to_next_day():
    return (86400 - time.time()) % 86400


def delay_to_next_week():
    return (86400 - time.time()) % (86400 * 7)


def delay_to_next_month():
    """ 31 days, not actually a month. """
    return (86400 - time.time()) % (86400 * 7 * 31)


class Scraper:
    def __init__(self, client: selfcord.Client, data_manager: DataManager,
                 sleep_delay=delay_to_next_month, message_fetch_limit=50):
        self.client = client
        self.data_manager = data_manager
        self.channel_server_id = {channel_id: self.client.get_channel(channel_id).guild.id for channel_id in self.data_manager}

        self.limit = message_fetch_limit
        self.sleep_time = sleep_delay

    async def begin_scraping(self):
        """
        Initiate scraping. Sets up the listener on new messages and scrapes provided channels
        """

        @self.client.event
        async def on_message(message: Message):
            if message.channel.id not in self.data_manager.channels:
                return
            await self._process_message(message)

        @self.client.event
        async def on_resume():
            await self.scrape_all_unseen()

        await on_resume()

        while True:
            await self.scrape_all_channels()
            await sleep(self.sleep_time())

    async def scrape_all_channels(self):
        """
        Scrape each channel provided in self.channels, expanding the frontier until done.
        Saves all messages to the message store.
        """
        current_channels = self.data_manager.get_targets()
        i = 0
        while current_channels:
            i %= len(current_channels)
            channel_empty = await self._scrape_channel(current_channels[i])
            if channel_empty:
                self.data_manager.finish_frontier(current_channels[i])
                current_channels.pop(i)
            i += 1

    async def scrape_all_unseen(self):
        """
        scrape each channel until a previously encountered message is found.
        this ensures no messages are missed until the next rescrape even if the script was offline for some time.
        Does not successfully recover if interrupted.
        """
        logging.info(f'Scraping unseen messages from all channels.')
        for channel in self.data_manager.channels:
            if self.data_manager.should_rescan(channel):
                continue
            await self._scrape_unseen_only(channel)
        logging.info(f'Finished scraping unseen messages.')

    async def _scrape_channel(self, channel_id: int):
        channel = client.get_channel(channel_id)
        frontier = self.data_manager.get_frontier_message(channel_id)
        logging.info(f"scraping channel: `{channel.name}` in server `{channel.guild.name}`")
        messages = [message async for message in channel.history(limit=self.limit, after=frontier, oldest_first=True)]
        await self._process_messages(messages)

        scraping_complete = len(messages) == 0 or len(messages) != self.limit
        return scraping_complete

    async def _scrape_unseen_only(self, channel_id: int):
        channel = client.get_channel(channel_id)
        messages = []
        async for message in channel.history(limit=self.limit, oldest_first=None):
            if self.data_manager.message_exists(message.id):
                return
            messages.append(message)
        await self._process_messages(messages)

    async def _process_message(self, message: Message, update_frontier: bool = False):
        await self.data_manager.save_message(message, update_frontier)
        logging.debug(message.content)

    async def _process_messages(self, messages: List[Message], update_frontier: bool = False):
        await self.data_manager.save_messages(messages, update_frontier)
        logging.info(f"processed {len(messages)} messages")
        logging.log(logging.INFO - 1, str([
            message.content for message in messages
        ]))


if __name__ == '__main__':
    file_handler = logging.FileHandler(filename='discord_scraper.log', encoding='utf-8', mode='w')
    stdout_handler = logging.StreamHandler(stdout)
    stdout_handler.setFormatter(_ColourFormatter())
    logging.basicConfig(handlers=[file_handler, stdout_handler], level=log_level, format='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s')
    client.run(token, log_level=log_level, log_handler=None)

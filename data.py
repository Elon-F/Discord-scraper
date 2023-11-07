import time
from abc import abstractmethod
from os import getenv
from typing import List, TypedDict, Union, Dict

from pymongo import MongoClient, UpdateOne
from selfcord import Object as DiscordObject, Message, Thread, MessageType


class Frontier(TypedDict):
    id: Union[int, None]
    previous_scan_time: int


class MessageConverter:
    async def convert_message(self, message: Message, thread: Thread = None) -> dict:
        """
        Convert a message into a format that can be stored.
        :param message:
        :param thread: Optional thread to associate with this message
        :return: Converted message
        """


class SimpleMessageConverter(MessageConverter):
    def __init__(self):
        self.authors = set()

    async def convert_message(self, message: Message, thread: Thread = None) -> dict:
        message_obj = {
            "channel_id": message.channel.id,
            "channel_type": str(message.channel.type),
            "message_id": message.id,
            "message_content": message.content,
            "timestamp": message.created_at,
            "edited_timestamp": message.edited_at,
            "attachments": [x.to_dict() for x in message.attachments],
            "embeds": [x.to_dict() for x in message.embeds],
            "author_id": message.author.id,
            "reply_to": None,
            "thread": None,
            "processed": False,
        }
        if message.reference:
            message_obj["reply_to"] = message.reference.to_dict()

        if thread:
            message_obj["thread"] = {
                "id": thread.id,
                "name": thread.name,
                "created_timestamp": thread.created_at,
                "message_count": thread.message_count,
                "owner_id": thread.owner_id,
                "messages": [(await self.convert_message(x)) async for x in thread.history()]
            }
        self.authors.add(message_obj["author_id"])
        return message_obj


class MongoMessageConverter(MessageConverter):
    def __init__(self):
        self.authors = set()

    async def convert_message(self, message: Message, thread: Thread = None):
        message_obj = {
            "channel_id": message.channel.id,
            "channel_type": str(message.channel.type),
            "message_id": message.id,
            "message_type": str(message.type),
            "timestamp": message.created_at,
            "edited_timestamp": message.edited_at,
            "attachments": [x.to_dict() for x in message.attachments],
            "embeds": [x.to_dict() for x in message.embeds],
            "author_id": message.author.id,
            "reply_to": None,
            "thread": None,
        }

        if message.reference:
            message_obj["reply_to"] = message.reference.to_dict()

        if message.edited_at:
            message_obj["edited_content"] = message.content

        if thread:
            message_obj["thread"] = {
                "id": thread.id,
                "name": thread.name,
                "created_timestamp": thread.created_at,
                "message_count": thread.message_count,
                "owner_id": thread.owner_id,
                "messages": [(await self.convert_message(x)) async for x in thread.history()]
            }

        self.authors.add(message_obj["author_id"])
        return {"$set": message_obj,
                "$setOnInsert": {"message_content": message.content,
                                 "processed": False}
                }


class DataStore:
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_frontiers(self) -> Dict[int, Frontier]:
        """
        :return: Dict mapping channel IDs to frontiers
        """

    @abstractmethod
    def get_frontier(self, channel_id: int) -> Frontier:
        """
        :param channel_id: Discord channel ID
        :return: Frontier for that channel
        """

    @abstractmethod
    def update_frontier(self, channel_id: int, new_message_id: int):
        """
        :param channel_id: Discord channel ID
        :param new_message_id: ID of the message which is the new frontier
        """

    @abstractmethod
    def save_message(self, message):
        """
        Save a message to the store
        """

    @abstractmethod
    def save_messages(self, messages: List):
        """
        Save multiple messages to the store
        """

    @abstractmethod
    def set_frontier(self, channel_id: int, data: Frontier):
        """
        Update the frontier for the given channel
        """

    @abstractmethod
    def message_exists(self, message_id: int) -> bool:
        """
        :return: Whether a message with message_id is present in the store
        """


class MongoStore(DataStore):
    def __init__(self):
        self.client = MongoClient(getenv("MONGO_HOST", "localhost"), int(getenv("MONGO_PORT", 27017)))
        self.db = self.client["discord_db"]
        self.messages = self.db["messages"]
        self.frontiers = self.db["frontiers"]
        self.channels = self.db["channels"]  # these three could just be a single collection i think. the point is to give me a mapping from channels to guilds so that i can build a URL, from authors to author data so that i can display it nicely, and same for guilds.
        self.authors = self.db["authors"]
        self.guilds = self.db["guilds"]

        # create indices
        self.messages.create_index("message_id", unique=True)
        self.messages.create_index("timestamp")

    def save_message(self, mongo_updater: dict):
        self.save_messages([mongo_updater])

    def save_reply(self, reply: dict):
        pass

    def save_messages(self, mongo_updater: List[dict]):
        self.messages.bulk_write([
            UpdateOne({"message_id": message["$set"]["message_id"]}, message, upsert=True) for message in mongo_updater
        ])
        replies = [
            UpdateOne({"message_id": message["$set"]["reply_to"]["message_id"]}, {"$addToSet": {"replies": message["$set"]["message_id"]}}, upsert=True) for message in mongo_updater if message["$set"]["reply_to"]
        ]
        if replies:
            self.messages.bulk_write(replies)

    def message_exists(self, message_id: int):
        return self.messages.find_one({"message_id": message_id}) is not None

    def get_frontier(self, channel_id: int):
        return self.frontiers.find_one({"channel_id": channel_id}) or {}

    def get_frontiers(self):
        return {x["channel_id"]: x for x in self.frontiers.find()}

    def update_frontier(self, channel_id: int, new_message_id: int):
        self.frontiers.update_one({"channel_id": channel_id}, {"$set": {"id": new_message_id}}, upsert=True)

    def set_frontier(self, channel_id: int, data: Frontier):
        self.frontiers.update_one({"channel_id": channel_id}, {"$set": data}, upsert=True)


class BasicStore(DataStore):
    def __init__(self):
        self.messages = []
        self.frontier = {}

    def save_message(self, message: dict):
        self.messages.append(message)

    def save_messages(self, messages: List):
        self.messages.extend(messages)

    def get_frontiers(self):
        return self.frontier

    def get_frontier(self, channel_id: int):
        return self.frontier.get(channel_id, {"id": None, "previous_scan_time": 0})

    def set_frontier(self, channel_id: int, data: dict):
        self.frontier[channel_id] = data

    def update_frontier(self, channel_id: int, new_message_id: int):
        self.frontier[channel_id] = {**self.get_frontier(channel_id), "id": new_message_id}

    def message_exists(self, message_id: int):
        return any(message_data["message_id"] == message_id for message_data in self.messages)


class DataManager:
    def __init__(self, target_channels: List[int],
                 store: DataStore = MongoStore(), converter: MessageConverter = MongoMessageConverter()):
        """
        :param target_channels: List of channels to be scraped
        :param store:
        :param converter:
        """
        self.channels = target_channels
        self.store = store
        self.converter = converter

        self.rescan_interval = {channel: 60 for channel in self.channels}

    def __iter__(self):
        yield from self.channels

    def get_targets(self):
        """
        :return: List of the channels that are currently set for scraping.
        """
        return [x for x in self.channels if self.get_frontier_message(x) or self.should_rescan(x)]

    def get_frontier_message(self, channel_id: int):
        """
        :param channel_id: The channel for which to get the frontier
        :return: DiscordObject() representing the message.
        """
        mid = self.get_frontier(channel_id).get("id", None)
        if mid:
            return DiscordObject(mid)

    def get_frontier(self, channel_id: int):
        return self.store.get_frontier(channel_id)

    def should_rescan(self, channel_id: int):
        """
        Determines whether a rescrape is due for the given channel
        """
        timestamp = self.get_frontier(channel_id).get("previous_scan_time", 0)
        return timestamp < time.time() - self.rescan_interval[channel_id]

    async def save_message(self, message: Message, update_frontier=False):
        """
        Saves the given message in the database, and updates the frontier if necessary.
        """
        if message.type != MessageType.thread_created:
            converted_message = await self.convert_message(message)
            self.store.save_message(converted_message)

        if update_frontier:
            self.store.update_frontier(message.channel.id, message.id)

    async def save_messages(self, messages: List[Message], update_frontier=False):
        """
        Saves the given message in the database, and updates the frontier if necessary.
        """
        converted_messages = [await self.convert_message(message) for message in messages if message.type != MessageType.thread_created]
        self.store.save_messages(converted_messages)

        last_message = messages[-1]
        if update_frontier:
            self.store.update_frontier(last_message.channel.id, last_message.id)

    async def convert_message(self, message: Message):
        """
        :return: Message converted to a storable representation
        """
        thread = message.channel.get_thread(message.id) if message.flags.has_thread else None
        return await self.converter.convert_message(message, thread)

    def finish_frontier(self, channel_id: int):
        """
        Marks the given channel scraping as complete and timestamps it.
        """
        front = self.get_frontier(channel_id)
        front["id"] = None
        front["previous_scan_time"] = int(time.time())
        self.store.set_frontier(channel_id, front)

    def message_exists(self, message_id: int):
        return self.store.message_exists(message_id)

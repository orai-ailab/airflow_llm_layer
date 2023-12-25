from dotenv import load_dotenv
import os

load_dotenv()
import argparse
import discord
from discord.ext import commands

parser = argparse.ArgumentParser()
parser.add_argument('--message', help='message to send')
args = parser.parse_args()


CHANNEL_ID = os.getenv('CHANNEL_ID')
TOKEN_BOT = os.getenv('TOKEN_DISCORD')

intents = discord.Intents.default()


client = discord.Client(intents=intents)

@client.event
async def on_ready():
    channel = client.get_channel(int(CHANNEL_ID))
    await channel.send(args.message)
    await client.close()
    
client.run(TOKEN_BOT)

        
        



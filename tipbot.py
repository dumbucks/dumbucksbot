import time
import asyncio
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from twitchio.ext import commands
from twitchio import User
from twitchio.http import TwitchHTTP
from twitchio import Client
from config import *
from database import *
from collections import defaultdict

w3 = Web3(Web3.HTTPProvider(INFURA_API_URL))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=CONTRACT_ABI)

class DUMBUCKSBot(commands.Bot):

    def __init__(self):
        super().__init__(token=TWITCH_OAUTH_TOKEN, prefix="~", initial_channels=[TWITCH_CHANNEL_NAME])
        self.active_users = {}
        self.recently_tipped = []
        self.bot_owner = "dumstream"

    async def event_ready(self):
        print(f"Bot connected to Twitch as {TWITCH_USERNAME}")
        await self.update_active_users()

    async def event_message(self, message):
        # Check if message.author is None
        if message.author is None:
            return

        # Ignore messages from the bot
        if message.author.name.lower() == TWITCH_USERNAME.lower():
            return

        # Update the user's last active time
        self.active_users[message.author.name] = time.time()

        # Handle the message
        await self.handle_commands(message)

    async def tip_user(self, username, amount):
        update_balance(username, amount)
        print(f"Tipped {username} {amount} DUMBUCKS")

    async def update_active_users(self):
        while True:
            self.active_users = {user: last_active_time for user, last_active_time in self.active_users.items()
                                if time.time() - last_active_time <= 900}

            await asyncio.sleep(900)

            active_users_in_last_minutes = get_active_users_in_last_minutes(self.active_users, 15)
            total_users_receiving = 0
            recipient_names = []

            # Calculate the amount of DUMBUCKS to be sent, excluding the bot owner
            total_amount = sum(1 for user in active_users_in_last_minutes if user.lower() != TWITCH_CHANNEL_NAME.lower())

            # Check if dumstream has enough DUMBUCKS to distribute
            dumstream_balance = get_balance(self.bot_owner)
            if dumstream_balance < total_amount:
                continue  # Skip this iteration if not enough DUMBUCKS are available

            for user in active_users_in_last_minutes:
                if user.lower() != TWITCH_CHANNEL_NAME.lower():  # Exclude the bot owner
                    await self.tip_user(user, 1)
                    total_users_receiving += 1
                    recipient_names.append(user)

            # Deduct the DUMBUCKS from dumstream
            update_balance(self.bot_owner, -total_amount)

            if total_users_receiving > 0:  # Check if there are active users
                recipients_str = ', '.join(recipient_names)
                message = f"1 DUMBUCK sent to {total_users_receiving} users for being active. Congrats {recipients_str}."
                await self.get_channel(TWITCH_CHANNEL_NAME).send(message)
    
    def get_total_dumbucks(self):
        conn = sqlite3.connect('dumbucks.db')
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(balance) FROM users")
        total = cursor.fetchone()[0]
        conn.close()
        return total



    # COMMANDS 

    @commands.command(name='send')
    async def give_command(self, ctx, username: str, amount_str: str):  # Change amount type to str
        try:
            amount = float(amount_str)
        except ValueError:
            await ctx.send(f"{ctx.author.name}, you must give a valid number as the amount of DUMBUCKS.")
            return

        amount = round(amount, 8)  # Round amount to the 8th decimal place

        if amount <= 0:
            await ctx.send(f"{ctx.author.name}, you must give a positive amount of DUMBUCKS.")
            return

        # Strip the @ symbol from the username
        username = username.lstrip('@')

        sender_balance = get_balance(ctx.author.name.lower())

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.name}, you don't have enough DUMBUCKS to give.")
            return

        update_balance(ctx.author.name.lower(), -amount)
        update_balance(username.lower(), amount)
        await ctx.send(f"{ctx.author.name} gave {username.lower()} {amount} DUMBUCKS.")

    @commands.command(name='balance', aliases=['wallet'])
    async def balance_command(self, ctx):
        user = ctx.author.name.lower()
        balance = get_balance(user)
        await ctx.send(f"{ctx.author.name}, your balance is {balance:.8f} DUMBUCKS.")  # Format balance to 8 decimal places    

    @commands.command(name='holders')
    async def count_users_command(self, ctx):
        conn = sqlite3.connect('dumbucks.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        conn.close()
        await ctx.send(f"There are {count} users with DUMBUCKS.")

    @commands.command(name='check')
    async def check_command(self, ctx, username: str):
        # Strip the @ symbol from the username
        username = username.lstrip('@')

        # Check if the username is empty or not provided
        if not username:
            await ctx.send(f"ERROR: Invalid usage of ~check command. Please provide a valid username after the command. Example: ~check @username")
            return

        balance = get_balance(username.lower())
        await ctx.send(f"{username} has {balance:.8f} DUMBUCKS.")  # Format balance to 8 decimal places

    @commands.command(name='help')
    async def help_command(self, ctx):
        help_message = (
            "Here are the available commands:\n"
            "~wallet: Check your balance. - \n"
            "~send [username] [amount]: Send DUMBUCKS to another user. - \n"
            "~holders: Show the total number of users with DUMBUCKS. - \n"
            "~check [username]: Check the balance of another user.\n"
        )
        await ctx.send(help_message)

    @commands.command(name='rain', aliases=['goldenshower'])
    async def rain_command(self, ctx, amount_str: str):  # Change 'amount' to a string
        try:
            amount = float(amount_str)
        except ValueError:
            await ctx.send(f"{ctx.author.name}, you must give a valid number as the amount of DUMBUCKS.")
            return

        sender = ctx.author.name.lower()

        if amount <= 0:
            await ctx.send(f"{ctx.author.name}, you must rain a positive amount of DUMBUCKS.")
            return

        sender_balance = get_balance(sender)

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.name}, you don't have enough DUMBUCKS to rain.")
            return

        # Get the list of active users in the last 10 minutes
        active_users = get_active_users_in_last_minutes(self.active_users, 10)

        user_count = len(active_users)
        amount_per_user = round(amount / user_count, 3)  # Round the amount to the thousandth decimal place

        for user in active_users:
            update_balance(user, amount_per_user)

        update_balance(sender, -amount)
        await ctx.send(f"{ctx.author.name} rained {amount_per_user:.3f} DUMBUCKS to {user_count} active users.")

    @commands.command(name='active')
    async def cmd_active(self, ctx):  # Add 'self' and 'ctx' here
        active_users = ', '.join(ctx.bot.active_users.keys())
        message = f"Active users: {active_users}" if active_users else "No active users."
        await ctx.send(message)


    # CHECK THE TOP 5 ON THE RICHLIST

    @commands.command(name='leaderboard', aliases=['richlist'])
    async def leaderboard_command(self, ctx):
        conn = sqlite3.connect('dumbucks.db')
        cursor = conn.cursor()
        cursor.execute("SELECT username, balance FROM users WHERE username != 'dumstream' ORDER BY balance DESC LIMIT 5")
        top_users = cursor.fetchall()
        conn.close()

        leaderboard_message = "Top 5 holders: \n"
        for i, (username, balance) in enumerate(top_users, start=1):
            leaderboard_message += f"{i}. {username}: {int(balance)} $DUM ~~~ \n"

        await ctx.send(leaderboard_message)

    # CHECK RANK ON LEADERBOARD

    @commands.command(name='rank')
    async def rank_command(self, ctx):
        conn = sqlite3.connect('dumbucks.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE balance > (SELECT balance FROM users WHERE username = ?)", (ctx.author.name.lower(),))
        rank = cursor.fetchone()[0] + 1  # Add 1 to account for 0-indexed list
        conn.close()

        await ctx.send(f"{ctx.author.name} is rank {rank} on the DUMBUCKS richlist.")
    
    # WITHDRAW DUMBUCKS TO ARBITRUM WALLET

    @commands.command(name='withdraw')
    async def withdraw_command(self, ctx, amount_str: str, eth_address: str):
        try:
            amount = float(amount_str)
        except ValueError:
            await ctx.send(f"{ctx.author.name}, you must give a valid number as the amount of DUMBUCKS.")
            return

        if amount <= 0:
            await ctx.send(f"{ctx.author.name}, you must withdraw a positive amount of DUMBUCKS.")
            return

        if not w3.is_address(eth_address):
            await ctx.send(f"{ctx.author.name}, you must provide a valid Ethereum address.")
            return

        sender_balance = get_balance(ctx.author.name.lower())

        if sender_balance < amount:
            await ctx.send(f"{ctx.author.name}, you don't have enough DUMBUCKS to withdraw.")
            return

        # Convert DUMBUCKS to the smallest unit (e.g., wei for Ether)
        amount_in_smallest_unit = int(amount * (10 ** 18))  # Assuming 18 decimals in your ERC20 token

        # Set up the transaction
        hot_wallet_address = HOT_WALLET_ADDRESS
        nonce = w3.eth.get_transaction_count(hot_wallet_address)

        # Estimate the gas required for the transaction
        estimated_gas = contract.functions.transfer(eth_address, amount_in_smallest_unit).estimate_gas({'from': hot_wallet_address})

        transfer_function = contract.functions.transfer(eth_address, amount_in_smallest_unit)
        transaction_dict = {
            'from': hot_wallet_address,
            'gas': estimated_gas,
            'gasPrice': w3.eth.gas_price,
            'nonce': nonce,
            'value': 0
        }
        transaction_dict.update({'data': transfer_function._encode_transaction_data(), 'to': CONTRACT_ADDRESS})

        # Sign and send the transaction
        signed_txn = Account.sign_transaction(transaction_dict, HOT_WALLET_PRIVATE_KEY)
        txn_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)

        # Update the user's balance in the database
        update_balance(ctx.author.name.lower(), -amount)

        # Build the message to send in the chat
        short_eth_address = eth_address[:5] + '...' + eth_address[-5:]
        transaction_link = f'https://arbiscan.io/tx/{txn_hash.hex()}'
        message = f"{ctx.author.name}, you have withdrawn {amount} DUMBUCKS to address {short_eth_address}. TX: {transaction_link}"

        await ctx.send(message)

    @commands.command(name='total')
    async def total_command(self, ctx):
        total_dumbucks = self.get_total_dumbucks()
        await ctx.send(f"The total amount of DUMBUCKS in the database is {total_dumbucks:.8f}.")


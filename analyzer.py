import argparse
import asyncio
import json
import os
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime as dt
from os.path import join
from signal import signal, SIGINT
from sqlite3 import OperationalError as sqliteoperr
from sys import exit
from time import sleep, time
import ftplib

from sqlalchemy import func, and_
from sqlalchemy.exc import OperationalError as sqloperr

from config import Config
from database import session, User, Event, Analyzer
from util import create_database, retrieve_files


class Analyze:
    def __init__(self):
        self.directory = Config.DIRECTORY
        self.session = session

    @staticmethod
    def last_file_scanned():
        try:
            return session.query(Analyzer).first()
        except sqloperr:
            create_database()
            return session.query(Analyzer).first()
        except sqliteoperr:
            create_database()
            return session.query(Analyzer).first()

    @staticmethod
    def set_last_scan():
        if len(session.query(Analyzer).all()) == 0:
            Analyze.create(last_scan=dt.utcnow())
        else:
            data = session.query(Analyzer).first()
            data.update(last_scan=dt.utcnow())

    @staticmethod
    def set_last_file(last_file):
        if len(session.query(Analyzer).all()) == 0:
            Analyzer.create(last_file=last_file)
        else:
            data = session.query(Analyzer).first()
            data.update(last_file=last_file)

    @staticmethod
    def get_files_list():
        files_list = []
        for root, dirs, files in os.walk(Config.DIRECTORY):
            for filename in files:
                if filename.endswith(".log"):
                    files_list.append(str(join(root, filename)))
        return files_list

    @staticmethod
    def remove_duplicate_events():
        subq = (session.query(Event.date, func.min(Event.id).label("min_id")).group_by(Event.date)).subquery(
            'date_min_id')
        duplicates = (session.query(Event).join(subq, and_(Event.date == subq.c.date, Event.id != subq.c.min_id)))
        for item in duplicates:
            print("Removing event " + str(item.id))
            session.delete(item)
        session.commit()

    @staticmethod
    def process_file(file):
        account_ipaddrs = {}
        regex_string_connect = re.compile(
            '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}) .+GameConnection::postConnectRoutine>.* IP:(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}):\d*  (\d*)')
        regex_string_disconnect = re.compile(
            '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}) .+NetInterface::sendDisconnectPacket.+IP:(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})')

        start = time()
        print("Analyzing log file " + str(file))
        with open(file, encoding="utf-8") as logfile:
            account_ids = []
            for line in logfile:
                for match in re.finditer(regex_string_connect, line):
                    # 0 = all, 1 = join time, 2 = ip, 3 = account id
                    # print(match.groups())

                    # Check list of account ids if not add it to list then query db and create if it doesn't exist.
                    if match.group(3) not in account_ids:
                        account_ids.append(match.group(3))
                        # check if it already exists from prior iteration
                        check = session.query(User).filter(User.account == match.group(3)).first()
                        if not check:
                            User.create(account=match.group(3))
                    account_ipaddrs[match.group(2)] = match.group(3)
                    Event.create(acctid=int(match.group(3)), state=1,
                                 date=dt.strptime(match.group(1), '%Y-%m-%d %H:%M:%S.%f'), ipaddr=match.group(2))
                for match in re.finditer(regex_string_disconnect, line):
                    # 0 = all, 1 = leave time, 2 = ip
                    # print(match.groups())

                    # # Get account from dict cache
                    # if match.group(2) in account_ipaddrs:
                    #     acctid = account_ipaddrs[match.group(2)]
                    Event.create(state=0, date=dt.strptime(match.group(1), '%Y-%m-%d %H:%M:%S.%f'),
                                 ipaddr=match.group(2))

            Analyze.set_last_file(file)
            Analyze.set_last_scan()
        end = time()
        print("Finished analyzing file " + str(file) + " in " + str(end - start)[:5] + " seconds.")

    @staticmethod
    def get_ips_by_account(acctid, list=False):
        _ip_list = []
        query = session.query(Event).filter_by(acctid=acctid).all()
        for account in query:
            if account.ipaddr not in _ip_list:
                _ip_list.append(account.ipaddr)
        return _ip_list if list else json.dumps({
            "acctid": acctid,
            "ip": _ip_list
        })

    @staticmethod
    def get_accounts_by_ip(ip, list=False):
        _accounts = []
        query = session.query(Event).filter_by(ipaddr=ip).all()
        for account in query:
            if account.acctid not in _accounts:
                _accounts.append(account.acctid)
        return _accounts if list else json.dumps({
            "ip": ip,
            "acctids": _accounts
        })

    @staticmethod
    def get_events_by_ip(ip):
        return session.query(Event).filter_by(ipaddr=ip).all()

    @staticmethod
    def get_accounts(account):
        accounts = []
        searched_accounts = []
        searched_ips = []

        ips = Analyze.get_ips_by_account(account, list=True)
        searched_accounts.append(account)

        def search_accounts(ips):
            for account in accounts:
                # print("search account: " + str(account))
                if account in searched_accounts:
                    # print("account already searched")
                    accounts.remove(account)
                    continue
                else:
                    query = Analyze.get_ips_by_account(account, list=True)
                    for ip in query:
                        if ip not in searched_ips:
                            # print("found new ip: " + ip)
                            ips.append(ip)
                    accounts.remove(account)
                    searched_accounts.append(account)

            for ip in ips:
                # print("search ip: " + ip)
                if ip in searched_ips:
                    # print("ip already searched")
                    ips.remove(ip)
                    continue
                else:
                    query = Analyze.get_events_by_ip(ip)
                    for event in query:
                        # get new ips
                        if event.ipaddr not in ips + searched_ips:
                            # print("found new ip: " + event.ipaddr)
                            ips.append(event.ipaddr)
                        # get new accounts
                        if event.acctid not in accounts + searched_accounts:
                            if event.acctid is not None:
                                # print("found new account: " + str(event.acctid))
                                accounts.append(event.acctid)
                    ips.remove(ip)
                    searched_ips.append(ip)

            if len(ips) > 0:
                # print("more ips exist, searching again")
                search_accounts(ips)

            if len(accounts) > 0:
                # print("more accounts exist, searching again")
                search_accounts(ips)

        search_accounts(ips)

        searched_accounts.sort()
        searched_ips.sort()
        return json.dumps({
            "accounts": searched_accounts,
            "searched_ips": searched_ips
        })

    @staticmethod
    def run():
        last_file_scanned = Analyze.last_file_scanned()
        all_files = Analyze.get_files_list()
        if last_file_scanned:
            remaining_files = all_files[all_files.index(last_file_scanned.last_file) + 1:]
        else:
            remaining_files = all_files
        if len(remaining_files) > 0:
            with ProcessPoolExecutor(max_workers=5) as executor:
                executor.map(Analyze.process_file, remaining_files)
            print("Completed parsing new log files.")
        else:
            print("Nothing to parse.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Life is Feudal Log File Analyzer for shared accounts.')
    parser.add_argument(dest="acct_id", nargs='?', type=int, help="Account ID to search.")
    parser.add_argument("-r", "--retrieve", action="store_true", help="Retrieve new log files.")
    parser.add_argument("-p", "--parse", action="store_true", help="Parse new log files.")
    parser.add_argument("-u", "--update", action="store_true", help="Download and parse new log files.")

    args = parser.parse_args()
    analyze = Analyze()


    def signal_handler(signal_received, frame):
        print("Terminating...this may take a minute.")
        session.commit()
        session.close()
        sleep(60)
        print("Session closed, exiting.")
        exit(0)

    def download():
        # retrieve_files()
        try:
            print("Downloading new log files.")
            retrieve_files()
            # asyncio.run(retrieve_files())
        except ftplib.all_errors:
            print("Could not connect or download new files.")


    def parse():
        try:
            print('Parsing new log files. Press CTRL-C to exit.')
            signal(SIGINT, signal_handler)
            analyze.run()
        except KeyboardInterrupt:
            print("Terminating...this may take a minute.")
            session.commit()
            session.close()
            # sleep(15)
            print("Session closed, exiting.")

    if args.acct_id:
        print(analyze.get_accounts(args.acct_id))
    if args.retrieve:
        download()
    if args.parse:
        parse()
    if args.update:
        download()
        parse()

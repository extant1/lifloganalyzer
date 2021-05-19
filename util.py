import ftplib
from os import makedirs
from os.path import join, exists

from config import Config
from datetime import datetime as dt
from database import Base, session, engine, User, Event, Analyzer
import asyncio
# from tqdm.asyncio import tqdm
from tqdm import tqdm
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer, UnknownLength, AdaptiveTransferSpeed


def retrieve_files():
    def find_files_cwd():
        log_files = []
        folders = []
        list = ftp.mlsd()
        for filename, data in list:
            if data['type'] == "dir":
                folders.append(filename)
            if data['type'] == "file":
                if filename.endswith(".log"):
                    log_files.append(filename)
        return log_files, folders

    pbar = 0

    def download_cwd_files(files, directory_name=None):
        if directory_name:
            DL_DIR = join(Config.DIRECTORY + directory_name)
        else:
            DL_DIR = Config.DIRECTORY

        for file in files:
            if not exists(DL_DIR):
                makedirs(DL_DIR)
            ftp.sendcmd("TYPE i")
            size = ftp.size(file)

            widgets = [file, Percentage(), ' ', Bar(marker=AnimatedMarker(fill="|"), left='[', right=']'), ' ',
                       Timer(), "/", AdaptiveETA(), ' ', AdaptiveTransferSpeed()]

            progress = ProgressBar(widgets=widgets, maxval=size)
            progress.start()

            fileout = open(join(DL_DIR, file), 'wb')

            def file_write(data):
                pbar = progress.value
                fileout.write(data)
                pbar += len(data)
                progress.update(pbar)

            # print(f"Simulating download of {file} as {join(DL_DIR, file)}")
            ftp.retrbinary(f"RETR {file}", file_write)
            print(f"Downloaded {join(DL_DIR, file)}")

        # async def download(file):
        #     if not exists(DL_DIR):
        #         print("making directory")
        #         makedirs(DL_DIR)
        #     ftp.sendcmd("TYPE i")
        #     size = ftp.size(file)
        #
        #     progress = tqdm(desc=file, total=size, unit="B", unit_scale=True, unit_divisor=1024)
        #
        #     def file_write(data):
        #         open(join(DL_DIR, file), 'wb').write(data)
        #         progress.update(len(data))
        #
        #     # print(f"Simulating download of {file} as {join(DL_DIR, file)}")
        #     ftp.retrbinary(f"RETR {file}", file_write)
        #     print(f"Downloaded {join(DL_DIR, file)}")
        #
        # await asyncio.gather(*[download(file) for file in files])

    ftp = ftplib.FTP()
    ftp.connect(Config.FTP_HOST, Config.FTP_PORT)
    ftp.login(Config.FTP_USER, Config.FTP_PASS)
    ftp.cwd("logs")

    files, folders = find_files_cwd()
    if len(files) > 0:
        download_cwd_files(files, ftp.pwd())
    for folder in folders:
        ftp.cwd(folder)
        files, folders = find_files_cwd()
        if len(files) > 0:
            download_cwd_files(files, folder)
        if len(folders) > 0:
            for folder in folders:
                ftp.cwd(folder)
                files, folders = find_files_cwd()
                if len(files) > 0:
                    download_cwd_files(files, folder)
                ftp.cwd("..")
        ftp.cwd("..")
    ftp.quit()
    print("Downloading new files complete.")


def create_database():
    from sqlalchemy.ext.declarative import declarative_base
    Base.metadata.create_all(engine)


def mock_data():
    # users
    User.create(account=12345)
    User.create(account=54321)
    # events
    Event.create(acctid=12345, state='1', date=dt(2020, 5, 17, 3, 20, 4), ipaddr="127.0.0.1")
    Event.create(acctid=12345, state='0', date=dt(2020, 5, 18, 3, 20, 4), ipaddr="127.0.0.1")
    Event.create(acctid=12345, state='1', date=dt(2020, 5, 19, 13, 22, 4), ipaddr="127.0.0.1")
    Event.create(acctid=54321, state='1', date=dt(2020, 5, 20, 4, 24, 4), ipaddr="123.1.0.123")
    Event.create(acctid=54321, state='1', date=dt(2020, 5, 21, 3, 25, 4), ipaddr="123.0.2.123")
    Event.create(acctid=54321, state='0', date=dt(2020, 5, 22, 1, 26, 4), ipaddr="123.0.0.123")
    Event.create(acctid=54321, state='0', date=dt(2020, 5, 23, 2, 32, 22), ipaddr="123.123.123.123")


def mock_dupe_events():
    Event.create(acctid=12345, state='1', date=dt(2020, 5, 17, 3, 20, 4), ipaddr="127.0.0.1")
    Event.create(acctid=12345, state='0', date=dt(2020, 5, 18, 3, 20, 4), ipaddr="127.0.0.1")
    Event.create(acctid=12345, state='1', date=dt(2020, 5, 19, 13, 22, 4), ipaddr="127.0.0.1")
    Event.create(acctid=54321, state='1', date=dt(2020, 5, 20, 4, 24, 4), ipaddr="123.1.0.123")
    Event.create(acctid=54321, state='1', date=dt(2020, 5, 21, 3, 25, 4), ipaddr="123.0.2.123")
    Event.create(acctid=54321, state='0', date=dt(2020, 5, 22, 1, 26, 4), ipaddr="123.0.0.123")
    Event.create(acctid=54321, state='0', date=dt(2020, 5, 23, 2, 32, 22), ipaddr="123.123.123.123")


def mock_data_extended():
    # users
    User.create(account=67890)
    User.create(account=99876)
    # events
    Event.create(acctid=67890, state='1', date=dt.utcnow(), ipaddr="127.0.0.1")
    Event.create(acctid=67890, state='0', date=dt.utcnow(), ipaddr="127.0.0.1")
    Event.create(acctid=67890, state='1', date=dt.utcnow(), ipaddr="127.0.0.1")
    Event.create(acctid=99876, state='1', date=dt.utcnow(), ipaddr="123.1.0.123")
    Event.create(acctid=99876, state='1', date=dt.utcnow(), ipaddr="123.0.2.123")
    Event.create(acctid=99876, state='0', date=dt.utcnow(), ipaddr="123.0.0.123")
    Event.create(acctid=99876, state='0', date=dt.utcnow(), ipaddr="123.123.123.123")

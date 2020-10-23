import os
from tqdm import tqdm # tqdm
import platform
import shutil
import re
import argparse
import asyncio
import aiohttp
import random
import subprocess
import multiprocessing
import time
from datetime import datetime, timedelta

import requests
import platform
import io
from datetime import datetime, timedelta

from lxml import etree          # lxml
from lxml.etree import QName    # lxml

s = requests.Session()

async def fetch(session, url, i, folder, pbar, sem):
    async with sem, session.get(url) as response:
        resp = await response.read()
        with open(os.path.join(folder, f"{str(i)}.ts"), "wb") as f:
            f.write(resp)
        pbar.update()

async def get_segments(total_segments, video_base, audio_base, tempdir):
    pbar = tqdm(total=2*len(total_segments), desc="Downloading segments")
    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(12)
        tasks = []
        for i in total_segments:
            tasks.append(asyncio.create_task(fetch(session, f"{video_base}{i}", i, os.path.join(tempdir, "temp-video"), pbar, sem)))
            tasks.append(asyncio.create_task(fetch(session, f"{audio_base}{i}", i, os.path.join(tempdir, "temp-audio"), pbar, sem)))
        await asyncio.wait(tasks)
    pbar.close()

def process_segments(params):
    ffmpeg_executable = params[0]
    tempdir = params[1]
    i = params[2]
    cmd_avseg = [
        ffmpeg_executable,
        "-y",
        "-i",
        f"{os.path.join(tempdir, 'temp-video', f'{i}.ts')}",
        "-i",
        f"{os.path.join(tempdir, 'temp-audio', f'{i}.ts')}",
        "-c",
        "copy",
        f"{os.path.join(tempdir, 'avseg')}/{i}.ts"
    ]
    proc = subprocess.Popen(cmd_avseg, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    proc.communicate()

class Stream:
    def __init__(self, stream_type, bitrate, codec, quality, base_url):
        self.stream_type = stream_type
        self.bitrate = bitrate
        self.codec = codec
        self.quality = quality
        self.base_url = base_url

    def __str__(self):
        return f"{self.quality:{' '}{'>'}{9}} Bitrate: {self.bitrate:{' '}{'>'}{8}} Codec: {self.codec}"

def local_to_utc(dt):
    if time.localtime().tm_isdst:
        return dt + timedelta(seconds=time.altzone)
    else:
        return dt + timedelta(seconds=time.timezone)


def get_mpd_data(video_url):
    page = s.get(video_url).text
    mpd_link = (
        page.split('dashManifestUrl\\":\\"')[-1].split('\\"')[0].replace("\/", "/")
    )
    return s.get(mpd_link).text


def process_mpd(mpd_data):
    tree = etree.parse(io.BytesIO(mpd_data.encode()))
    root = tree.getroot()
    nsmap = {(k or "def"): v for k, v in root.nsmap.items()}
    time = root.attrib[QName(nsmap["yt"], "mpdResponseTime")]
    d_time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
    total_seg = (
        int(root.attrib[QName(nsmap["yt"], "earliestMediaSequence")])
        + len(tree.findall(".//def:S", nsmap))
        - 1
    )
    attribute_sets = tree.findall(".//def:Period/def:AdaptationSet", nsmap)
    v_streams = []
    a_streams = []
    for a in attribute_sets:
        stream_type = a.attrib["mimeType"][0]
        for r in a.findall(".//def:Representation", nsmap):
            bitrate = int(r.attrib["bandwidth"])
            codec = r.attrib["codecs"]
            base_url = r.find(".//def:BaseURL", nsmap).text + "sq/"
            if stream_type == "a":
                quality = r.attrib["audioSamplingRate"]
                a_streams.append(Stream(stream_type, bitrate, codec, quality, base_url))
            elif stream_type == "v":
                quality = f"{r.attrib['width']}x{r.attrib['height']}"
                v_streams.append(Stream(stream_type, bitrate, codec, quality, base_url))
    a_streams.sort(key=lambda x: x.bitrate, reverse=True)
    v_streams.sort(key=lambda x: x.bitrate, reverse=True)
    return a_streams, v_streams, total_seg, d_time

def info(a, v, m, s):
    print(f"You can go back {int(m*2/3600)} hours and {int(m*2%3600/60)} minutes...")
    print(f"Download avaliable from {datetime.today() - timedelta(seconds=m*2)}")
    print("\nAudio stream ids")
    for i in range(len(a)):
        print(f"{i}:  {str(a[i])}")

    print("\nVideo stream ids")
    for i in range(len(v)):
        print(f"{i}:  {str(v[i])}")
    
    print("\nUse format -1 for no video or audio")

def parse_datetime(inp, utc=True):
    formats = ["%Y-%m-%dT%H:%M", "%d.%m.%Y %H:%M", "%d.%m %H:%M", "%H:%M", "%Y-%m-%dT%H:%M:%S", "%d.%m.%Y %H:%M:%S", "%d.%m %H:%M:%S", "%H:%M:%S"]
    for fmt in formats:
        try:
            d_time = datetime.strptime(inp, fmt)
            today = datetime.today()
            if not ('d' in fmt):
                d_time = d_time.replace(year=today.year, month=today.month,day=today.day)
            if not ('Y' in fmt):
                d_time = d_time.replace(year=today.year)
            if utc:
                return d_time
            return local_to_utc(d_time)
        except ValueError:
            pass
    return -1

def parse_duration(inp):
    x = re.findall("([0-9]+[hmsHMS])", inp)
    if len(x) == 0:
        try:
            number = int(inp)
        except:
            return -1
        return number
    else:
        total_seconds = 0
        for chunk in x:
            if chunk[-1] == "h":
                total_seconds += int(chunk[:-1]) * 3600
            elif chunk[-1] == "m":
                total_seconds += int(chunk[:-1]) * 60
            elif chunk[-1] == "s":
                total_seconds += int(chunk[:-1])
        return total_seconds

def main(ffmpeg_executable, ffprobe_executable):
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='OUTPUT_FILE', action='store', help='The output filename')
    parser.add_argument('-s', '--start', metavar='START_TIME', action='store', help='The start time (possible formats = "12:34", "12:34:56", "7.8.2009 12:34:56", "2009-08-07T12:34:56")')
    parser.add_argument('-e', '--end', metavar='END_TIME', action='store', help='The end time (same format as start time)')
    parser.add_argument('-d', '--duration', action='store', help='The duration (possible formats = "12h34m56s", "12m34s", "123s", "123m", "123h", ...)')
    parser.add_argument('-u', '--utc', action='store_true', help='Use UTC instead of local time for start and end time', default=False)
    parser.add_argument('-l', '--list-formats', action='store_true', help='List info about stream ids', default=False)
    parser.add_argument('-af', action='store', help='Select audio stream id', type=int, default=0)
    parser.add_argument('-vf', action='store', help='Select video stream id', type=int, default=0)
    parser.add_argument('-y', '--overwrite', action='store_true', help='Overwrite file without asking', default=False)
    parser.add_argument('url', metavar='URL', action='store', help='The URL of the YouTube stream')
    args = parser.parse_args()
    url = args.url
    output_path = args.output
    start_time = None
    duration_secs = None

    if output_path:
        formats = (".mp4", ".mkv", ".aac")
        if not output_path.endswith(formats):
            print("Error: Unsupported output file format!")
            print("Supported file formats are:")
            for f in formats:
                print(f"\t{f}")
            exit(1)

    mpd_data = get_mpd_data(url)
    a, v, m, s = process_mpd(mpd_data)

    if args.list_formats == True:
        info(a, v, m, s)
        return

    # if args.vf == -1:
    #     video_url = ""
    # else:
    #     video_url = v[args.vf].base_url
    # if args.af == -1:
    #     audio_url = ""
    # else:
    #     audio_url = a[args.af].base_url
    video_url = v[args.vf].base_url
    audio_url = a[args.af].base_url

    result = subprocess.run([ffprobe_executable, "-i", f"{video_url}/{m}", "-show_entries", "format=start_time,duration", "-v", "quiet", "-of", "csv=p=0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    seg_start, seg_end = (map(float, result.stdout.rstrip().decode("utf-8").split(",")))
    seg_len = seg_end - seg_start

    start_time = (
        s - timedelta(seconds=m * seg_len)
        if args.start == None
        else parse_datetime(args.start, args.utc)
    )

    if start_time == -1:
        print("Error: Couldn't parse start date!")
        exit(1)

    if args.duration == None and args.end == None:
        duration = m * seg_len
    else:
        duration = (
            parse_datetime(args.end, args.utc)
            if args.duration == None
            else parse_duration(args.duration)
        )

    if duration == -1:
        print("Error: Couldn't parse duration or end date!")
        exit(1)

    start_segment = m - round((s - start_time).total_seconds() / seg_len)
    if start_segment < 0:
        start_segment = 0

    end_segment = start_segment + round(duration / seg_len)
    if end_segment > m:
        print("Error: You are requesting segments that dont exist yet!")
        exit(1)
    total_segments = range(start_segment, end_segment)

    if os.path.exists(output_path):
        if args.overwrite:
            os.remove(output_path)
        else:
            while True:
                print(f'File "{output_path}" already exists! Overwrite? [y/N] ', end='')
                yn = input().lower()
                if yn == '' or yn == 'n':
                    exit(0)
                else:
                    os.remove(output_path)
                    break

    # make a temporary directory in the output file's directory
    tempdir_parent = os.path.dirname(os.path.abspath(os.path.realpath(output_path)))
    tempdir = ".temp-" + str(random.randint(1000,9999))
    while os.path.exists(tempdir):
        tempdir = ".temp-" + str(random.randint(1000,9999))
    tempdir = os.path.join(tempdir_parent, tempdir)
    os.mkdir(tempdir)

    os.mkdir(os.path.join(tempdir, "temp-video"))
    os.mkdir(os.path.join(tempdir, "temp-audio"))

    # get video and audio segments asynchronously
    asyncio.get_event_loop().run_until_complete(get_segments(total_segments, video_url, audio_url, tempdir))

    # merge video and audio segments each into its file
    os.mkdir(os.path.join(tempdir, "avseg"))
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        params = ((ffmpeg_executable, tempdir, i) for i in total_segments)
        list(tqdm(pool.imap_unordered(process_segments, params), total=len(total_segments), desc="Merging segments"))
        pool.close()
        pool.join()
    
    with open(os.path.join(tempdir, "avseg.txt"), "w+") as avseg:
        for i in total_segments:
            avseg.write(f"file '{os.path.join(tempdir, 'avseg', f'{i}.ts')}'\n")

    shutil.rmtree(os.path.join(tempdir, "temp-video"), ignore_errors=True)
    shutil.rmtree(os.path.join(tempdir, "temp-audio"), ignore_errors=True)

    size_avseg_total = sum(os.path.getsize(os.path.join(tempdir, "avseg", f)) for f in os.listdir(os.path.join(tempdir, "avseg")) if os.path.isfile(os.path.join(tempdir, "avseg", f)))//1024
    cmd_final = [
        ffmpeg_executable,
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        f"{os.path.join(tempdir, 'avseg.txt')}",
        "-c",
        "copy",
        output_path
    ]
    process_final = subprocess.Popen(cmd_final, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    pbar_final = tqdm(total=size_avseg_total, desc="Merging final")

    while True:
        line_avseg = process_final.stdout.readline()
        if not line_avseg: break

        match_avseg = re.match(r".*size= *(\d+)", line_avseg)
        if match_avseg is not None:
            size_avseg = int(match_avseg[1])
            pbar_final.update(size_avseg - pbar_final.n)

    pbar_final.update(size_avseg_total - pbar_final.n)
    pbar_final.close()

    shutil.rmtree(tempdir, ignore_errors=True)

if __name__ == "__main__":
    plt = platform.system()
    if plt == "Windows":
        if not (os.path.exists("./bin/ffmpeg.exe") or shutil.which("ffmpeg") or os.path.exists("./bin/ffprobeexe") or shutil.which("ffprobe")):
            print("Run 'python download.py' first!")
            exit(1)
        elif os.path.exists("./bin/ffmpeg.exe") and os.path.exists("./bin/ffprobe.exe"):
            main(".\\bin\\ffmpeg.exe", ".\\bin\\ffprobe.exe")
        else:
            main("ffmpeg", "ffprobe")
    elif plt == "Linux" or plt == "Darwin":
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            print("Install ffmpeg to path!")
            exit(1)
        else:
            main("ffmpeg", "ffprobe")

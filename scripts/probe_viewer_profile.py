"""Bounded live field-preservation probe; persist counts only, no viewer values."""
import argparse
import collections
import io
import json
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from collector_events import NdjsonSink
from liveMan import DouyinLiveWebFetcher


def run(room, seconds, output):
    import os
    os.chdir(ROOT)
    counts = collections.Counter()
    lock = threading.Lock()
    connected = threading.Event()
    def sink(event):
        with lock:
            counts[event['type'] + '.events'] += 1
            if event['type'] == 'source.connected': connected.set()
            if event['type'] == 'collector.error':
                counts['errors.' + str(event['payload'].get('errorCategory','unknown'))] += 1
    fetcher = DouyinLiveWebFetcher(room,event_sink=sink)
    original = fetcher._emit_data
    def observe(*args, **kwargs):
        event = original(*args, **kwargs)
        user = kwargs.get('user')
        if user is None: return event
        stream=io.StringIO()
        NdjsonSink(stream).emit(event)
        decoded=json.loads(stream.getvalue())
        profile=decoded['actor'].get('observedProfile',{})
        kind=decoded['type']
        # Independent direct field checks for valid, non-default incoming observations.
        expected={}
        if user.display_id and user.display_id.isprintable() and not any(c.isspace() for c in user.display_id) and len(user.display_id)<=256:
            expected['displayId']=user.display_id
        urls=user.avatar_thumb.url_list_list
        if urls and urls[0].startswith('https://') and len(urls[0])<=4096 and not any(c.isspace() for c in urls[0]):
            expected['avatarUrl']=urls[0]
        for attr,key in [('follower_count','followerCount'),('following_count','followingCount')]:
            value=getattr(user.follow_info,attr)
            if 0 < value <= 9007199254740991: expected[key]=value
        with lock:
            for key in profile: counts[kind+'.output.'+key] += 1
            for key,value in expected.items():
                counts[kind+'.rawPresent.'+key] += 1
                counts[kind+('.equal.' if profile.get(key)==value else '.mismatch.')+key] += 1
        return event
    fetcher._emit_data=observe
    failures=[]
    def collect():
        try: fetcher.start()
        except Exception as e: failures.append(type(e).__name__)
    started=int(time.time()*1000)
    worker=threading.Thread(target=collect,daemon=True)
    worker.start()
    opened=connected.wait(25)
    if opened: time.sleep(seconds)
    fetcher.stop();worker.join(timeout=8)
    if worker.is_alive():failures.append('StopDeadlineExceeded')
    report={'webRid':room,'startedAt':started,'finishedAt':int(time.time()*1000),
            'connected':opened,'secondsRequested':seconds,'errors':failures,'counts':dict(counts)}
    report['fieldMismatches']=sum(v for k,v in counts.items() if '.mismatch.' in k)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if opened and not failures and not report['fieldMismatches'] and not any(k.startswith('errors.') for k in counts) else 1


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('room');p.add_argument('--seconds',type=int,default=30)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    if not args.room.isdecimal() or not 5<=args.seconds<=120:p.error('numeric room and seconds 5..120 required')
    sys.exit(run(args.room,args.seconds,args.output.resolve()))

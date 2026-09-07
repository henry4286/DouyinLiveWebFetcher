#!/usr/bin/python
# coding:utf-8

# @FileName:    liveMan.py
# @Time:        2024/1/2 21:51
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

import codecs
import gzip
import hashlib
import io
import random
import re
import string
import subprocess
import sys
import threading
import time
import execjs
import urllib.parse
from contextlib import contextmanager
from unittest.mock import patch

import requests
import websocket
from py_mini_racer import MiniRacer

from ac_signature import get__ac_signature
from collector_events import CallbackSink, actor_fields, build_collector_event
from protobuf.douyin import *

from urllib3.util.url import parse_url


HTTP_REQUEST_TIMEOUT = (5, 15)
MAX_COMPRESSED_PUSH_BYTES = 1024 * 1024
MAX_DECOMPRESSED_PUSH_BYTES = 8 * 1024 * 1024


def decompress_gzip_bounded(payload):
    """Decompress one Webcast payload without allowing unbounded expansion."""
    if len(payload) > MAX_COMPRESSED_PUSH_BYTES:
        raise ValueError('compressed push payload is too large')
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        body = stream.read(MAX_DECOMPRESSED_PUSH_BYTES + 1)
    if len(body) > MAX_DECOMPRESSED_PUSH_BYTES:
        raise ValueError('decompressed push payload is too large')
    return body


def execute_js(js_file: str):
    """
    执行 JavaScript 文件
    :param js_file: JavaScript 文件路径
    :return: 执行结果
    """
    with open(js_file, 'r', encoding='utf-8') as file:
        js_code = file.read()
    
    ctx = execjs.compile(js_code)
    return ctx


@contextmanager
def patched_popen_encoding(encoding='utf-8'):
    original_popen_init = subprocess.Popen.__init__
    
    def new_popen_init(self, *args, **kwargs):
        kwargs['encoding'] = encoding
        original_popen_init(self, *args, **kwargs)
    
    with patch.object(subprocess.Popen, '__init__', new_popen_init):
        yield


def generateSignature(wss, script_file='sign.js'):
    """
    出现gbk编码问题则修改 python模块subprocess.py的源码中Popen类的__init__函数参数encoding值为 "utf-8"
    """
    params = ("live_id,aid,version_code,webcast_sdk_version,"
              "room_id,sub_room_id,sub_channel_id,did_rule,"
              "user_unique_id,device_platform,device_type,ac,"
              "identity").split(',')
    wss_params = urllib.parse.urlparse(wss).query.split('&')
    wss_maps = {i.split('=')[0]: i.split("=")[-1] for i in wss_params}
    tpl_params = [f"{i}={wss_maps.get(i, '')}" for i in params]
    param = ','.join(tpl_params)
    md5 = hashlib.md5()
    md5.update(param.encode())
    md5_param = md5.hexdigest()
    
    with codecs.open(script_file, 'r', encoding='utf8') as f:
        script = f.read()
    
    ctx = MiniRacer()
    ctx.eval(script)
    
    try:
        signature = ctx.call("get_sign", md5_param)
        return signature
    except Exception as e:
        print(
            f"signature generation failed errorClass={type(e).__name__}",
            file=sys.stderr,
        )
        raise
    
    # 以下代码对应js脚本为sign_v0.js
    # context = execjs.compile(script)
    # with patched_popen_encoding(encoding='utf-8'):
    #     ret = context.call('getSign', {'X-MS-STUB': md5_param})
    # return ret.get('X-Bogus')


def generateMsToken(length=182):
    """
    产生请求头部cookie中的msToken字段，其实为随机的107位字符
    :param length:字符位数
    :return:msToken
    """
    random_str = ''
    base_str = string.ascii_letters + string.digits + '-_'
    _len = len(base_str) - 1
    for _ in range(length):
        random_str += base_str[random.randint(0, _len)]
    return random_str


class DouyinLiveWebFetcher:
    
    def __init__(self, live_id, abogus_file='a_bogus.js', event_sink=None):
        """
        直播间弹幕抓取对象
        :param live_id: 直播间的直播id，打开直播间web首页的链接如：https://live.douyin.com/261378947940，
                        其中的261378947940即是live_id
        """
        self.abogus_file = abogus_file
        self.__ttwid = None
        self.__room_id = None
        self.session = requests.Session()
        # Collector URLs are fixed by this adapter.  Do not import ambient
        # proxy or .netrc credentials from the host process.
        self.session.trust_env = False
        self.live_id = live_id
        self.host = "https://www.douyin.com/"
        self.live_url = "https://live.douyin.com/"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        self.headers = {
            'User-Agent': self.user_agent
        }
        if event_sink is not None and not hasattr(event_sink, 'emit'):
            event_sink = CallbackSink(event_sink)
        self.event_sink = event_sink
        self.ws = None
        self._stop_event = threading.Event()
        self._stop_lock = threading.Lock()
        self._heartbeat_thread = None
        self._stopping_emitted = False
        self._room_resolved_emitted = False
        self._connection_epoch = 0
        self._reconnect_attempt = 0

    def _human_log(self, message):
        """Preserve the original console output when no structured sink is set."""
        if self.event_sink is None:
            print(message)

    @staticmethod
    def _diagnostic(message):
        print(message, file=sys.stderr)

    def _publish(self, event, human_message=None):
        if self.event_sink is not None:
            self.event_sink.emit(event)
        elif human_message is not None:
            self._human_log(human_message)
        return event

    def _lifecycle_payload(self, error_category=None):
        return {
            'connectionEpoch': self._connection_epoch,
            'reconnectAttempt': self._reconnect_attempt,
            'errorCategory': error_category,
        }

    def _emit_lifecycle(self, event_type, *, method=None,
                        error_category=None, human_message=None,
                        platform_event_id=None, platform_occurred_at=None,
                        received_at=None):
        event = build_collector_event(
            kind='lifecycle',
            event_type=event_type,
            room_id=self.__room_id,
            web_rid=self.live_id,
            payload=self._lifecycle_payload(error_category),
            method=method or event_type,
            platform_event_id=platform_event_id,
            platform_occurred_at=platform_occurred_at,
            received_at=received_at,
        )
        return self._publish(event, human_message)

    def _emit_error(self, category, error, *, method=None):
        error_class = type(error).__name__ if error is not None else None
        self._diagnostic(
            f"collector error category={category} "
            f"method={method or 'collector.error'} "
            f"errorClass={error_class}"
        )
        return self._emit_lifecycle(
            'collector.error',
            error_category=category,
        )

    def _emit_data(self, event_type, message, payload, *, method,
                   outer_msg_id=None, received_at=None, user=None,
                   common_attr='common', human_message=None):
        common = getattr(message, common_attr, None)
        actor, actor_quality = actor_fields(user)
        platform_event_id = getattr(common, 'msg_id', None) or outer_msg_id
        platform_time = getattr(common, 'create_time', None)
        event_room_id = getattr(common, 'room_id', None) or self.__room_id
        event = build_collector_event(
            kind='data',
            event_type=event_type,
            room_id=event_room_id,
            web_rid=self.live_id,
            payload=payload,
            method=method,
            actor=actor,
            platform_event_id=platform_event_id,
            platform_occurred_at=platform_time,
            received_at=received_at,
            actor_id_quality=actor_quality,
        )
        return self._publish(event, human_message)
    
    def start(self):
        # The optional stdin controller starts before this blocking method.  It
        # may already have received EOF (for example when its parent exits), in
        # which case starting a new network connection would orphan the
        # collector that the controller just stopped.
        if self._stop_event.is_set():
            return
        self._emit_lifecycle('collector.starting')
        try:
            if self._stop_event.is_set():
                return
            self._connectWebSocket()
        except Exception as error:
            if not self._stop_event.is_set():
                self._emit_error('connection_failure', error)
            raise
        finally:
            self.stop()
    
    def stop(self):
        with self._stop_lock:
            if self._stop_event.is_set():
                return
            self._stop_event.set()
            emit_stopping = not self._stopping_emitted
            self._stopping_emitted = True
            ws = self.ws

        if emit_stopping:
            try:
                self._emit_lifecycle('collector.stopping')
            except Exception as error:
                self._diagnostic(
                    "collector stopping observer failed "
                    f"errorClass={type(error).__name__}"
                )

        if ws is not None:
            try:
                ws.close()
            except Exception as error:
                # Cleanup must not call the same failing event sink recursively.
                self._diagnostic(
                    "websocket close failed "
                    f"errorClass={type(error).__name__}"
                )

        heartbeat = self._heartbeat_thread
        if (heartbeat is not None and heartbeat.is_alive()
                and heartbeat is not threading.current_thread()):
            heartbeat.join(timeout=6)
    
    @property
    def ttwid(self):
        """
        产生请求头部cookie中的ttwid字段，访问抖音网页版直播间首页可以获取到响应cookie中的ttwid
        :return: ttwid
        """
        if self.__ttwid:
            return self.__ttwid
        headers = {
            "User-Agent": self.user_agent,
        }
        try:
            response = self.session.get(
                self.live_url, headers=headers, timeout=HTTP_REQUEST_TIMEOUT
            )
            response.raise_for_status()
        except Exception as err:
            self._emit_error('ttwid_request_failure', err)
            raise
        else:
            self.__ttwid = response.cookies.get('ttwid')
            return self.__ttwid
    
    @property
    def room_id(self):
        """
        根据直播间的地址获取到真正的直播间roomId，有时会有错误，可以重试请求解决
        :return:room_id
        """
        if self.__room_id:
            return self.__room_id
        url = self.live_url + self.live_id
        headers = {
            "User-Agent": self.user_agent,
            "cookie": f"ttwid={self.ttwid}&msToken={generateMsToken()}; __ac_nonce=0123407cc00a9e438deb4",
        }
        try:
            response = self.session.get(
                url, headers=headers, timeout=HTTP_REQUEST_TIMEOUT
            )
            response.raise_for_status()
        except Exception as err:
            self._emit_error('room_resolve_request_failure', err)
            raise
        else:
            match = re.search(r'roomId\\":\\"(\d+)\\"', response.text)
            if match is None or len(match.groups()) < 1:
                error = RuntimeError('room id was not present in the response')
                self._emit_error('room_id_missing', error)
                raise error
            
            self.__room_id = match.group(1)
            if not self._room_resolved_emitted:
                self._room_resolved_emitted = True
                self._emit_lifecycle(
                    'room.resolved',
                    human_message=f"【√】已解析直播间ID: {self.__room_id}",
                )
            
            return self.__room_id
    
    def get_ac_nonce(self):
        """
        获取 __ac_nonce
        """
        resp_cookies = self.session.get(
            self.host, headers=self.headers, timeout=HTTP_REQUEST_TIMEOUT
        ).cookies
        return resp_cookies.get("__ac_nonce")
    
    def get_ac_signature(self, __ac_nonce: str = None) -> str:
        """
        获取 __ac_signature
        """
        __ac_signature = get__ac_signature(self.host[8:], __ac_nonce, self.user_agent)
        self.session.cookies.set("__ac_signature", __ac_signature)
        return __ac_signature
    
    def get_a_bogus(self, url_params: dict):
        """
        获取 a_bogus
        """
        url = urllib.parse.urlencode(url_params)
        ctx = execute_js(self.abogus_file)
        _a_bogus = ctx.call("get_ab", url, self.user_agent)
        return _a_bogus
    
    def get_room_status(self):
        """
        获取直播间开播状态:
        room_status: 2 直播已结束
        room_status: 0 直播进行中
        """
        msToken = generateMsToken()
        nonce = self.get_ac_nonce()
        signature = self.get_ac_signature(nonce)
        url = ('https://live.douyin.com/webcast/room/web/enter/?aid=6383'
               '&app_name=douyin_web&live_id=1&device_platform=web&language=zh-CN&enter_from=page_refresh'
               '&cookie_enabled=true&screen_width=5120&screen_height=1440&browser_language=zh-CN&browser_platform=Win32'
               '&browser_name=Edge&browser_version=140.0.0.0'
               f'&web_rid={self.live_id}'
               f'&room_id_str={self.room_id}'
               '&enter_source=&is_need_double_stream=false&insert_task_id=&live_reason=&msToken=' + msToken)
        query = parse_url(url).query
        params = {i[0]: i[1] for i in [j.split('=') for j in query.split('&')]}
        a_bogus = self.get_a_bogus(params)  # 计算a_bogus,成功率不是100%，出现失败时重试即可
        url += f"&a_bogus={a_bogus}"
        headers = self.headers.copy()
        headers.update({
            'Referer': f'https://live.douyin.com/{self.live_id}',
            'Cookie': f'ttwid={self.ttwid};__ac_nonce={nonce}; __ac_signature={signature}',
        })
        resp = self.session.get(
            url, headers=headers, timeout=HTTP_REQUEST_TIMEOUT
        )
        data = resp.json().get('data')
        if data:
            room_status = data.get('room_status')
            user = data.get('user')
            user_id = user.get('id_str')
            nickname = user.get('nickname')
            self._human_log(
                f"【{nickname}】[{user_id}]直播间："
                f"{['正在直播', '已结束'][bool(room_status)]}."
            )
    
    def _connectWebSocket(self):
        """
        连接抖音直播间websocket服务器，请求直播间数据
        """
        if self._stop_event.is_set():
            return
        wss = ("wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?app_name=douyin_web"
               "&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
               "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true"
               "&screen_width=1536&screen_height=864&browser_language=zh-CN&browser_platform=Win32"
               "&browser_name=Mozilla"
               "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,"
               "%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
               "&browser_online=true&tz_name=Asia/Shanghai"
               "&cursor=d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"
               f"&internal_ext=internal_src:dim|wss_push_room_id:{self.room_id}|wss_push_did:7319483754668557238"
               f"|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1|wss_info:0-1721106114633-0-0|"
               f"wrds_v:7392094459690748497"
               f"&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1"
               f"&user_unique_id=7319483754668557238&im_path=/webcast/im/fetch/&identity=audience"
               f"&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id={self.room_id}&heartbeatDuration=0")
        
        signature = generateSignature(wss)
        wss += f"&signature={signature}"
        
        headers = {
            "cookie": f"ttwid={self.ttwid}",
            'user-agent': self.user_agent,
        }
        self.ws = websocket.WebSocketApp(wss,
                                         header=headers,
                                         on_open=self._wsOnOpen,
                                         on_message=self._wsOnMessage,
                                         on_error=self._wsOnError,
                                         on_close=self._wsOnClose)
        self.ws.run_forever()
    
    def _sendHeartbeat(self):
        """
        发送心跳包
        """
        while not self._stop_event.is_set():
            try:
                heartbeat = PushFrame(payload_type='hb').SerializeToString()
                self.ws.send(heartbeat, websocket.ABNF.OPCODE_PING)
                self._human_log("【√】发送心跳包")
            except Exception as e:
                if not self._stop_event.is_set():
                    self._emit_error('heartbeat_send_failure', e)
                break
            self._stop_event.wait(5)
    
    def _wsOnOpen(self, ws):
        """
        连接建立成功
        """
        if self._stop_event.is_set():
            return
        self._connection_epoch += 1
        self._emit_lifecycle(
            'source.connected',
            human_message="【√】WebSocket连接成功.",
        )
        if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive():
            self._heartbeat_thread = threading.Thread(
                target=self._sendHeartbeat,
                name=f'douyin-heartbeat-{self.live_id}',
                daemon=True,
            )
            self._heartbeat_thread.start()
    
    def _wsOnMessage(self, ws, message):
        """
        接收到数据
        :param ws: websocket实例
        :param message: 数据
        """
        
        received_at = int(time.time() * 1000)
        try:
            # 根据proto结构体解析对象
            package = PushFrame().parse(message)
            response = Response().parse(decompress_gzip_bounded(package.payload))
        except Exception as error:
            self._emit_error('envelope_parse_failure', error,
                             method='webcast.envelope')
            return
        
        # 返回直播间服务器链接存活确认消息，便于持续获取数据
        if response.need_ack:
            ack = PushFrame(log_id=package.log_id,
                            payload_type='ack',
                            payload=response.internal_ext.encode('utf-8')
                            ).SerializeToString()
            try:
                ws.send(ack, websocket.ABNF.OPCODE_BINARY)
            except Exception as error:
                self._emit_error('ack_send_failure', error,
                                 method='webcast.ack')
        
        # 根据消息类别解析消息体
        parsers = {
            'WebcastChatMessage': self._parseChatMsg,  # 聊天消息
            'WebcastGiftMessage': self._parseGiftMsg,  # 礼物消息
            'WebcastLikeMessage': self._parseLikeMsg,  # 点赞消息
            'WebcastMemberMessage': self._parseMemberMsg,  # 进入直播间消息
            'WebcastSocialMessage': self._parseSocialMsg,  # 关注消息
            'WebcastRoomUserSeqMessage': self._parseRoomUserSeqMsg,  # 直播间统计
            'WebcastFansclubMessage': self._parseFansclubMsg,  # 粉丝团消息
            'WebcastControlMessage': self._parseControlMsg,  # 直播间状态消息
            'WebcastEmojiChatMessage': self._parseEmojiChatMsg,  # 聊天表情包消息
            'WebcastRoomStatsMessage': self._parseRoomStatsMsg,  # 直播间统计信息
            'WebcastRoomMessage': self._parseRoomMsg,  # 直播间信息
            'WebcastRoomRankMessage': self._parseRankMsg,  # 直播间排行榜信息
            'WebcastRoomStreamAdaptationMessage': self._parseRoomStreamAdaptationMsg,
        }
        for msg in response.messages_list:
            method = msg.method
            parser = parsers.get(method)
            if parser is None:
                safe_method = ''.join(
                    char for char in str(method)
                    if char.isprintable() and char not in '\r\n'
                )[:128]
                self._diagnostic(
                    f"unknown webcast method method={safe_method!r}"
                )
                continue
            try:
                parser(msg.payload, method=method, outer_msg_id=msg.msg_id,
                       received_at=received_at)
            except Exception as error:
                self._emit_error('message_parse_failure', error, method=method)

    def _wsOnError(self, ws, error):
        if not self._stop_event.is_set():
            self._emit_error('websocket_error', error,
                             method='source.connected')
    
    def _wsOnClose(self, ws, *args):
        self._emit_lifecycle(
            'source.disconnected',
            error_category=None if self._stop_event.is_set() else 'unexpected_close',
            human_message="WebSocket connection closed.",
        )

    def _parseChatMsg(self, payload, *, method='WebcastChatMessage',
                      outer_msg_id=None, received_at=None):
        """聊天消息"""
        message = ChatMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        content = message.content
        return self._emit_data(
            'comment', message, {'content': content}, method=method,
            outer_msg_id=outer_msg_id, received_at=received_at,
            user=message.user,
            human_message=f"【聊天msg】[{user_id}]{user_name}: {content}",
        )

    def _parseGiftMsg(self, payload, *, method='WebcastGiftMessage',
                      outer_msg_id=None, received_at=None):
        """礼物消息"""
        message = GiftMessage().parse(payload)
        user_name = message.user.nick_name
        gift_name = message.gift.name
        gift_id = message.gift_id or message.gift.id or None
        gift_cnt = message.combo_count or message.repeat_count or 1
        return self._emit_data(
            'gift', message,
            {
                'giftId': str(gift_id) if gift_id is not None else None,
                'giftName': gift_name or None,
                'giftCount': int(gift_cnt),
                # Value semantics have not been validated, so do not infer
                # monetary value from diamond_count/fan_ticket_count.
                'giftValue': None,
                'comboCount': int(message.combo_count),
                'repeatCount': int(message.repeat_count),
                'repeatEnd': int(message.repeat_end),
                'groupId': str(message.group_id) if message.group_id else None,
                'countSemantics': 'unverified',
            },
            method=method, outer_msg_id=outer_msg_id,
            received_at=received_at, user=message.user,
            human_message=f"【礼物msg】{user_name} 送出了 {gift_name}x{gift_cnt}",
        )

    def _parseLikeMsg(self, payload, *, method='WebcastLikeMessage',
                      outer_msg_id=None, received_at=None):
        '''点赞消息'''
        message = LikeMessage().parse(payload)
        user_name = message.user.nick_name
        count = message.count
        observed_payload = {'likeCount': int(count)}
        if 0 < message.total <= 9007199254740991:
            observed_payload['totalLikeCount'] = int(message.total)
        return self._emit_data(
            'like', message, observed_payload, method=method,
            outer_msg_id=outer_msg_id, received_at=received_at,
            user=message.user,
            human_message=f"【点赞msg】{user_name} 点了{count}个赞",
        )

    def _parseMemberMsg(self, payload, *, method='WebcastMemberMessage',
                        outer_msg_id=None, received_at=None):
        '''进入直播间消息'''
        message = MemberMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        gender = {0: "未知", 1: "男", 2: "女"}.get(message.user.gender, "未知")
        return self._emit_data(
            'enter_room', message, {}, method=method,
            outer_msg_id=outer_msg_id, received_at=received_at,
            user=message.user,
            human_message=f"【进场msg】[{user_id}][{gender}]{user_name} 进入了直播间",
        )

    def _parseSocialMsg(self, payload, *, method='WebcastSocialMessage',
                        outer_msg_id=None, received_at=None):
        '''社交消息；action 语义验证前不进入结构化业务通道。'''
        message = SocialMessage().parse(payload)
        user_name = message.user.nick_name
        user_id = message.user.id
        if self.event_sink is None:
            self._human_log(
                f"【社交msg】[{user_id}]{user_name} 发生社交互动"
            )
        else:
            # Safe diagnostic: do not expose the actor or claim that an
            # unverified numeric action means follow/unfollow.
            return self._emit_data(
                'social', message, {'action': int(message.action), 'semantics': 'unverified'},
                method=method, outer_msg_id=outer_msg_id, received_at=received_at, user=message.user,
            )
        return None

    def _parseRoomUserSeqMsg(self, payload, *,
                             method='WebcastRoomUserSeqMessage',
                             outer_msg_id=None, received_at=None):
        '''直播间统计'''
        message = RoomUserSeqMessage().parse(payload)
        current = message.total
        total = message.total_pv_for_anchor
        try:
            total_viewers = int(total)
        except (TypeError, ValueError):
            total_viewers = None
        if self.event_sink is None:
            self._human_log(
                f"【统计msg】当前观看人数: {current}, 累计观看人数: {total}"
            )
        else:
            return self._emit_data(
                'room_stats', message,
                {'onlineCount': int(current) if current >= 0 else None,
                 'totalViewerCount': total_viewers if total_viewers is not None and total_viewers >= 0 else None},
                method=method, outer_msg_id=outer_msg_id, received_at=received_at,
            )
        return None

    def _parseFansclubMsg(self, payload, *, method='WebcastFansclubMessage',
                          outer_msg_id=None, received_at=None):
        '''粉丝团消息'''
        message = FansclubMessage().parse(payload)
        content = message.content or None
        level = getattr(getattr(message.user.fans_club, 'data', None),
                        'level', None)
        return self._emit_data(
            'fansclub', message,
            {
                'fansclubLevel': int(level) if level else None,
                'fansclubReasonType': int(message.type) if message.type else None,
                'content': content,
            },
            method=method, outer_msg_id=outer_msg_id,
            received_at=received_at, user=message.user,
            common_attr='common_info',
            human_message=f"【粉丝团msg】 {content or ''}",
        )

    def _parseEmojiChatMsg(self, payload, **_context):
        '''聊天表情包消息'''
        message = EmojiChatMessage().parse(payload)
        emoji_id = message.emoji_id
        user = message.user
        common = message.common
        default_content = message.default_content
        self._human_log(
            f"【聊天表情包id】 {emoji_id},user：{user},common:{common},"
            f"default_content:{default_content}"
        )

    def _parseRoomMsg(self, payload, **_context):
        message = RoomMessage().parse(payload)
        common = message.common
        room_id = common.room_id
        self._human_log(f"【直播间msg】直播间id:{room_id}")
    
    def _parseRoomStatsMsg(self, payload, *, method='WebcastRoomStatsMessage',
                           outer_msg_id=None, received_at=None):
        message = RoomStatsMessage().parse(payload)
        display_long = message.display_long
        if self.event_sink is None:
            self._human_log(f"【直播间统计msg】{display_long}")
        else:
            total = int(message.total) if 0 <= message.total <= 9007199254740991 else None
            return self._emit_data(
                'room_stats', message, {
                    'onlineCount': None, 'totalViewerCount': None,
                    'observedTotal': total,
                    'displayText': message.display_long[:256] or None,
                    'displayValue': int(message.display_value) if 0 <= message.display_value <= 9007199254740991 else None,
                    'displayTypeRaw': int(message.display_type) if 0 <= message.display_type <= 9007199254740991 else None,
                    'countSemantics': 'unverified',
                },
                method=method, outer_msg_id=outer_msg_id, received_at=received_at,
            )
        return None

    def _parseRankMsg(self, payload, **_context):
        message = RoomRankMessage().parse(payload)
        rank_count = len(message.ranks_list)
        if self.event_sink is None:
            self._human_log(f"【直播间排行榜msg】共{rank_count}项（内容已省略）")
        else:
            self._diagnostic(f"room rank update count={rank_count}")
    
    def _parseControlMsg(self, payload, *, method='WebcastControlMessage',
                         outer_msg_id=None, received_at=None):
        '''直播间状态消息'''
        message = ControlMessage().parse(payload)

        if message.status == 3:
            common = message.common
            try:
                self._emit_lifecycle(
                    'room.ended',
                    method=method,
                    platform_event_id=common.msg_id or outer_msg_id,
                    platform_occurred_at=common.create_time,
                    received_at=received_at,
                    human_message="直播间已结束",
                )
            finally:
                self.stop()

    def _parseRoomStreamAdaptationMsg(self, payload, **_context):
        message = RoomStreamAdaptationMessage().parse(payload)
        adaptationType = message.adaptation_type
        self._human_log(f'直播间adaptation: {adaptationType}')

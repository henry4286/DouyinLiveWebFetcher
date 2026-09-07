import io
import gzip
import json
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout

from collector_events import (
    CallbackSink,
    NdjsonSink,
    SCHEMA_VERSION,
    build_collector_event,
)
from liveMan import (
    HTTP_REQUEST_TIMEOUT,
    MAX_COMPRESSED_PUSH_BYTES,
    MAX_DECOMPRESSED_PUSH_BYTES,
    DouyinLiveWebFetcher,
    decompress_gzip_bounded,
)
from main import (
    CONTROL_COMMAND_MAX_CHARS,
    StdinStopController,
    configure_ndjson_stdout,
    run_collector,
)
from protobuf.douyin import (
    ChatMessage,
    Common,
    ControlMessage,
    GiftMessage,
    GiftStruct,
    LikeMessage,
    Message,
    PushFrame,
    Response,
    RoomRankMessage,
    RoomRankMessageRoomRank,
    RoomStatsMessage,
    RoomUserSeqMessage,
    SocialMessage,
    User,
)


NOW_MS = 1_788_512_400_123
PLATFORM_MS = 1_788_512_400_000


def encoded(message):
    return message.SerializeToString()


class RecordingSink:
    def __init__(self):
        self.events = []

    def emit(self, event):
        self.events.append(event)


class CollectorEventBuilderTests(unittest.TestCase):
    def test_contract_root_fields_and_platform_id(self):
        event = build_collector_event(
            kind='data',
            event_type='comment',
            room_id=123,
            web_rid='456',
            payload={'content': 'hello'},
            method='WebcastChatMessage',
            platform_event_id=789,
            platform_occurred_at=PLATFORM_MS,
            received_at=NOW_MS,
        )

        self.assertEqual(
            set(event),
            {
                'schemaVersion', 'kind', 'eventId', 'roomId', 'webRid',
                'type', 'occurredAt', 'receivedAt', 'actor', 'payload',
                'source', 'quality',
            },
        )
        self.assertEqual(event['schemaVersion'], SCHEMA_VERSION)
        self.assertEqual(event['eventId'], '789')
        self.assertEqual(event['roomId'], '123')
        self.assertEqual(event['occurredAt'], PLATFORM_MS)
        self.assertEqual(event['quality']['eventId'], 'platform')
        self.assertEqual(event['quality']['occurredAt'], 'platform')

    def test_fallback_id_is_deterministic_and_unverified_time_degrades(self):
        arguments = dict(
            kind='data',
            event_type='like',
            room_id='123',
            web_rid='456',
            payload={'likeCount': 2},
            method='WebcastLikeMessage',
            platform_occurred_at=1_788_512_400,  # seconds are not guessed
            received_at=NOW_MS,
        )
        first = build_collector_event(**arguments)
        second = build_collector_event(**arguments)

        self.assertEqual(first['eventId'], second['eventId'])
        self.assertRegex(first['eventId'], r'^fallback:[0-9a-f]{64}$')
        self.assertEqual(first['occurredAt'], NOW_MS)
        self.assertEqual(first['quality']['eventId'], 'fallback')
        self.assertEqual(first['quality']['occurredAt'], 'fallback')
        later_observation = build_collector_event(
            **{**arguments, 'received_at': NOW_MS + 1}
        )
        self.assertNotEqual(first['eventId'], later_observation['eventId'])

    def test_callback_sink_accepts_plain_callable(self):
        received = []
        CallbackSink(received.append).emit({'type': 'comment'})
        self.assertEqual(received, [{'type': 'comment'}])

    def test_callback_sink_serializes_concurrent_callbacks(self):
        state_lock = threading.Lock()
        active = 0
        maximum_active = 0

        def callback(_event):
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.002)
            with state_lock:
                active -= 1

        sink = CallbackSink(callback)
        callers = [threading.Thread(target=sink.emit, args=({'i': i},))
                   for i in range(20)]
        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join(timeout=1)

        self.assertEqual(maximum_active, 1)


class FetcherParserTests(unittest.TestCase):
    def setUp(self):
        self.sink = RecordingSink()
        self.fetcher = DouyinLiveWebFetcher('456', event_sink=self.sink)

    def test_http_session_does_not_inherit_host_credentials_or_proxies(self):
        self.assertFalse(self.fetcher.session.trust_env)

    def test_http_room_bootstrap_uses_bounded_connect_and_read_timeout(self):
        calls = []

        class ResponseStub:
            cookies = {'ttwid': 'temporary-cookie'}

            @staticmethod
            def raise_for_status():
                return None

        class SessionStub:
            @staticmethod
            def get(*args, **kwargs):
                calls.append((args, kwargs))
                return ResponseStub()

        self.fetcher.session = SessionStub()

        self.assertEqual(self.fetcher.ttwid, 'temporary-cookie')
        self.assertEqual(calls[0][1]['timeout'], HTTP_REQUEST_TIMEOUT)

    def test_webcast_gzip_payload_has_compressed_and_expanded_limits(self):
        expected = b'bounded payload'
        self.assertEqual(decompress_gzip_bounded(gzip.compress(expected)), expected)

        with self.assertRaisesRegex(ValueError, 'compressed'):
            decompress_gzip_bounded(b'x' * (MAX_COMPRESSED_PUSH_BYTES + 1))
        with self.assertRaisesRegex(ValueError, 'decompressed'):
            decompress_gzip_bounded(gzip.compress(
                b'x' * (MAX_DECOMPRESSED_PUSH_BYTES + 1)
            ))

    def test_comment_never_exports_unverified_source_user_id(self):
        message = ChatMessage(
            common=Common(msg_id=10, room_id=123,
                          create_time=PLATFORM_MS),
            user=User(id=987654321, sec_uid='MS4wLjAB-synthetic_only', nick_name='viewer'),
            content='这套搭配为什么这样选',
        )
        event = self.fetcher._parseChatMsg(
            encoded(message), received_at=NOW_MS
        )

        self.assertEqual(event['type'], 'comment')
        self.assertEqual(event['payload'], {'content': '这套搭配为什么这样选'})
        self.assertEqual(event['eventId'], '10')
        self.assertEqual(event['actor'], {
            'sourceUserId': None,
            'nickname': 'viewer',
            'observedSourceUserId': '987654321',
            'observedSecUid': 'MS4wLjAB-synthetic_only',
        })
        self.assertEqual(event['quality']['actorId'], 'unavailable')

    def test_like_uses_increment(self):
        message = LikeMessage(
            common=Common(msg_id=11, room_id=123,
                          create_time=PLATFORM_MS),
            user=User(nick_name='viewer'),
            count=7,
            total=99,
        )
        event = self.fetcher._parseLikeMsg(
            encoded(message), received_at=NOW_MS
        )

        self.assertEqual(event['payload'], {'likeCount': 7, 'totalLikeCount': 99})
        self.assertNotIn('total', event['payload'])

    def test_observed_ids_reject_placeholders_and_preserve_uint64_as_text(self):
        for user_id in (0, 111111, 18446744073709551615):
            event = self.fetcher._parseChatMsg(encoded(ChatMessage(
                common=Common(msg_id=123, room_id=123), user=User(id=user_id), content='fixture')))
            self.assertIsNone(event['actor']['sourceUserId'])
            self.assertEqual(event['quality']['actorId'], 'unavailable')
            if user_id in (0, 111111):
                self.assertNotIn('observedSourceUserId', event['actor'])
            else:
                self.assertEqual(event['actor']['observedSourceUserId'], str(user_id))

    def test_gift_emits_minimal_fields_without_guessing_value(self):
        message = GiftMessage(
            common=Common(msg_id=12, room_id=123,
                          create_time=PLATFORM_MS),
            gift_id=88,
            combo_count=3,
            user=User(nick_name='viewer'),
            gift=GiftStruct(id=88, name='星星', diamond_count=9),
        )
        event = self.fetcher._parseGiftMsg(
            encoded(message), received_at=NOW_MS
        )

        self.assertEqual(event['payload'], {
            'giftId': '88',
            'giftName': '星星',
            'giftCount': 3,
            'giftValue': None,
            'comboCount': 3, 'repeatCount': 0, 'repeatEnd': 0,
            'groupId': None, 'countSemantics': 'unverified',
        })

    def test_room_end_emits_lifecycle_then_stops_once(self):
        message = ControlMessage(
            common=Common(msg_id=13, room_id=123,
                          create_time=PLATFORM_MS),
            status=3,
        )
        self.fetcher._parseControlMsg(encoded(message), received_at=NOW_MS)
        self.fetcher.stop()

        types = [event['type'] for event in self.sink.events]
        self.assertEqual(types, ['room.ended', 'collector.stopping'])
        self.assertEqual(self.sink.events[0]['eventId'], '13')
        self.assertTrue(self.fetcher._stop_event.is_set())

    def test_start_does_not_connect_after_controller_already_stopped(self):
        self.fetcher.stop()
        self.fetcher._connectWebSocket = lambda: self.fail(
            'a pre-stopped collector must not establish a connection'
        )

        self.fetcher.start()

        self.assertEqual(
            [event['type'] for event in self.sink.events],
            ['collector.stopping'],
        )

    def test_connected_disconnected_and_recyclable_daemon_heartbeat(self):
        self.fetcher._sendHeartbeat = lambda: None
        self.fetcher._wsOnOpen(object())
        thread = self.fetcher._heartbeat_thread
        thread.join(timeout=1)
        self.fetcher._wsOnClose(object(), 1000, 'normal')
        self.fetcher.stop()

        self.assertTrue(thread.daemon)
        self.assertFalse(thread.is_alive())
        self.assertEqual(
            [event['type'] for event in self.sink.events],
            ['source.connected', 'source.disconnected', 'collector.stopping'],
        )
        self.assertEqual(
            self.sink.events[0]['payload']['connectionEpoch'], 1
        )

    def test_invalid_envelope_is_observable(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.fetcher._wsOnMessage(object(), b'not-a-push-frame')

        self.assertEqual(self.sink.events[-1]['type'], 'collector.error')
        self.assertEqual(
            self.sink.events[-1]['payload']['errorCategory'],
            'envelope_parse_failure',
        )
        self.assertIn('envelope_parse_failure', stderr.getvalue())

    def test_message_parser_failure_is_observable(self):
        response = Response(messages_list=[
            Message(method='WebcastChatMessage', payload=b'\xff', msg_id=91)
        ])
        frame = PushFrame(payload=gzip.compress(encoded(response)))
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            self.fetcher._wsOnMessage(object(), encoded(frame))

        error = self.sink.events[-1]
        self.assertEqual(error['type'], 'collector.error')
        self.assertEqual(error['source']['method'], 'collector.error')
        self.assertEqual(error['payload']['errorCategory'],
                         'message_parse_failure')
        self.assertIn('WebcastChatMessage', stderr.getvalue())

    def test_unknown_method_diagnostic_strips_control_characters(self):
        response = Response(messages_list=[
            Message(method='Unknown\nforged-log', payload=b'', msg_id=93)
        ])
        frame = PushFrame(payload=gzip.compress(encoded(response)))
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            self.fetcher._wsOnMessage(object(), encoded(frame))

        diagnostic = stderr.getvalue()
        self.assertIn('Unknownforged-log', diagnostic)
        self.assertNotIn('Unknown\nforged-log', diagnostic)

    def test_ack_failure_does_not_drop_messages_in_same_response(self):
        comment = ChatMessage(
            common=Common(msg_id=92, room_id=123,
                          create_time=PLATFORM_MS),
            user=User(nick_name='viewer'),
            content='still parsed',
        )
        response = Response(
            need_ack=True,
            internal_ext='ack-payload',
            messages_list=[Message(
                method='WebcastChatMessage', payload=encoded(comment), msg_id=92
            )],
        )
        frame = PushFrame(log_id=7, payload=gzip.compress(encoded(response)))

        class FailingAckSocket:
            def send(self, *_args, **_kwargs):
                raise OSError('send failed')

        with redirect_stderr(io.StringIO()):
            self.fetcher._wsOnMessage(FailingAckSocket(), encoded(frame))

        self.assertEqual(
            [event['type'] for event in self.sink.events],
            ['collector.error', 'comment'],
        )
        self.assertEqual(self.sink.events[-1]['payload']['content'],
                         'still parsed')

    def test_rank_output_is_summarized(self):
        message = RoomRankMessage(
            common=Common(msg_id=14),
            ranks_list=[
                RoomRankMessageRoomRank(
                    user=User(nick_name='private-name'), score_str='123'
                )
            ],
        )
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.fetcher._parseRankMsg(encoded(message))

        self.assertIn('count=1', stderr.getvalue())
        self.assertNotIn('private-name', stderr.getvalue())

    def test_stop_is_idempotent_across_threads(self):
        callers = [threading.Thread(target=self.fetcher.stop)
                   for _ in range(5)]
        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join(timeout=1)

        self.assertEqual(
            [event['type'] for event in self.sink.events],
            ['collector.stopping'],
        )

    def test_failing_stopping_observer_does_not_block_cleanup(self):
        class FailingSink:
            def emit(self, _event):
                raise RuntimeError('observer unavailable')

        class FakeSocket:
            closed = False

            def close(self):
                self.closed = True

        fetcher = DouyinLiveWebFetcher('456', event_sink=FailingSink())
        fetcher.ws = FakeSocket()
        heartbeat = threading.Thread(
            target=lambda: fetcher._stop_event.wait(5), daemon=True
        )
        fetcher._heartbeat_thread = heartbeat
        heartbeat.start()

        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            fetcher.stop()

        self.assertTrue(fetcher.ws.closed)
        self.assertFalse(heartbeat.is_alive())
        self.assertIn('observer failed', diagnostics.getvalue())

    def test_social_and_stats_emit_observations_not_follow_or_user_identity(self):
        social = SocialMessage(
            common=Common(msg_id=17),
            user=User(nick_name='must-not-leak'),
            action=1,
        )
        user_sequence = RoomUserSeqMessage(
            common=Common(msg_id=18), total=12, total_pv_for_anchor='34'
        )
        room_stats = RoomStatsMessage(
            common=Common(msg_id=19), display_long='private display', total=56
        )
        diagnostics = io.StringIO()

        with redirect_stderr(diagnostics):
            self.fetcher._parseSocialMsg(encoded(social))
            self.fetcher._parseRoomUserSeqMsg(encoded(user_sequence))
            self.fetcher._parseRoomStatsMsg(encoded(room_stats))

        self.assertEqual([e['type'] for e in self.sink.events], ['social', 'room_stats', 'room_stats'])
        self.assertEqual(self.sink.events[0]['payload'], {'action': 1, 'semantics': 'unverified'})
        self.assertEqual(self.sink.events[1]['payload'], {'onlineCount': 12, 'totalViewerCount': 34})
        self.assertEqual(self.sink.events[2]['payload'], {
            'onlineCount': None, 'totalViewerCount': None, 'observedTotal': 56,
            'displayText': 'private display', 'displayValue': 0, 'displayTypeRaw': 0,
            'countSemantics': 'unverified'})
        self.assertNotIn('must-not-leak', diagnostics.getvalue())
        self.assertNotIn('private display', diagnostics.getvalue())
        self.assertNotIn('follow', [e['type'] for e in self.sink.events])


class NdjsonIsolationTests(unittest.TestCase):
    def test_stdout_contains_only_complete_json_event_lines(self):
        stdout = io.StringIO()
        diagnostics = io.StringIO()
        fetcher = DouyinLiveWebFetcher(
            '456', event_sink=NdjsonSink(stdout)
        )
        comment = ChatMessage(
            common=Common(msg_id=15, room_id=123,
                          create_time=PLATFORM_MS),
            user=User(nick_name='viewer'),
            content='hello',
        )
        rank = RoomRankMessage(
            ranks_list=[RoomRankMessageRoomRank(user=User(nick_name='secret'))]
        )

        with redirect_stdout(io.StringIO()), redirect_stderr(diagnostics):
            fetcher._parseChatMsg(encoded(comment), received_at=NOW_MS)
            fetcher._parseRankMsg(encoded(rank))

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])['type'], 'comment')
        self.assertNotIn('secret', diagnostics.getvalue())

    def test_default_mode_preserves_human_log(self):
        fetcher = DouyinLiveWebFetcher('456')
        message = LikeMessage(
            common=Common(msg_id=16),
            user=User(nick_name='viewer'),
            count=2,
        )
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            fetcher._parseLikeMsg(encoded(message), received_at=NOW_MS)

        self.assertIn('【点赞msg】viewer 点了2个赞', stdout.getvalue())
        self.assertNotIn(SCHEMA_VERSION, stdout.getvalue())

    def test_concurrent_ndjson_writes_are_complete_lines(self):
        class FragmentingStream:
            def __init__(self):
                self.value = ''

            def write(self, text):
                midpoint = len(text) // 2
                self.value += text[:midpoint]
                time.sleep(0.0005)
                self.value += text[midpoint:]

            def flush(self):
                return None

            def getvalue(self):
                return self.value

        stdout = FragmentingStream()
        sink = NdjsonSink(stdout)
        callers = [
            threading.Thread(
                target=sink.emit,
                args=({'schemaVersion': SCHEMA_VERSION, 'index': index,
                       'content': f'中文🙂-{index}'},),
            )
            for index in range(100)
        ]

        for caller in callers:
            caller.start()
        for caller in callers:
            caller.join(timeout=1)

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 100)
        decoded = [json.loads(line) for line in lines]
        self.assertEqual({event['index'] for event in decoded}, set(range(100)))

    def test_cli_reconfigures_non_utf8_stdout_for_chinese_and_emoji(self):
        raw = io.BytesIO()
        text = io.TextIOWrapper(raw, encoding='cp1252', newline='')
        configured = configure_ndjson_stdout(text)
        sink = NdjsonSink(configured)

        sink.emit({'content': '中文🙂'})
        configured.flush()
        raw_bytes = raw.getvalue()

        self.assertEqual(configured.encoding.lower().replace('-', ''), 'utf8')
        self.assertEqual(json.loads(raw_bytes.decode('utf-8'))['content'], '中文🙂')

    def test_ndjson_sink_is_safe_on_explicit_cp1252_stream(self):
        raw = io.BytesIO()
        text = io.TextIOWrapper(raw, encoding='cp1252', newline='')
        sink = NdjsonSink(text)

        sink.emit({'content': '中文🙂'})
        text.flush()
        wire = raw.getvalue()

        self.assertTrue(wire.isascii())
        self.assertEqual(json.loads(wire.decode('utf-8'))['content'], '中文🙂')

    def test_default_ndjson_sink_is_safe_with_cp1252_stdout(self):
        raw = io.BytesIO()
        text = io.TextIOWrapper(raw, encoding='cp1252', newline='')

        with redirect_stdout(text):
            NdjsonSink().emit({'content': '中文🙂'})
        text.flush()
        wire = raw.getvalue()

        self.assertTrue(wire.isascii())
        self.assertEqual(json.loads(wire.decode('utf-8'))['content'], '中文🙂')


class StdinStopControllerTests(unittest.TestCase):
    class FakeRoom:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    def run_control(self, value):
        room = self.FakeRoom()
        diagnostics = io.StringIO()
        controller = StdinStopController(
            room,
            input_stream=io.StringIO(value),
            diagnostic_stream=diagnostics,
        )
        controller.run()
        return room, diagnostics.getvalue()

    def test_exact_stop_command_stops_without_diagnostic(self):
        room, diagnostics = self.run_control('stop\r\n')

        self.assertEqual(room.stop_calls, 1)
        self.assertEqual(diagnostics, '')

    def test_unterminated_carriage_return_is_not_exact_stop(self):
        room, diagnostics = self.run_control('stop\r')

        self.assertEqual(room.stop_calls, 1)
        self.assertIn('control_command_unknown', diagnostics)

    def test_eof_uses_same_stop_path(self):
        room, diagnostics = self.run_control('')

        self.assertEqual(room.stop_calls, 1)
        self.assertEqual(diagnostics, '')

    def test_unknown_command_is_categorized_without_echo(self):
        room, diagnostics = self.run_control('sensitive-value\nstop\n')

        self.assertEqual(room.stop_calls, 1)
        self.assertIn('control_command_unknown', diagnostics)
        self.assertNotIn('sensitive-value', diagnostics)

    def test_overlong_command_is_drained_without_echo(self):
        secret = 'sensitive-' + ('x' * CONTROL_COMMAND_MAX_CHARS)
        room, diagnostics = self.run_control(secret + '\nstop\n')

        self.assertEqual(room.stop_calls, 1)
        self.assertIn('control_input_too_long', diagnostics)
        self.assertNotIn(secret, diagnostics)

    def test_unknown_unterminated_command_reports_then_stops_at_eof(self):
        room, diagnostics = self.run_control('unknown')

        self.assertEqual(room.stop_calls, 1)
        self.assertIn('control_command_unknown', diagnostics)

    def test_control_diagnostics_never_use_ndjson_stdout(self):
        stdout = io.StringIO()
        room = self.FakeRoom()
        diagnostics = io.StringIO()

        with redirect_stdout(stdout):
            StdinStopController(
                room,
                input_stream=io.StringIO('not-stop\nstop\n'),
                diagnostic_stream=diagnostics,
            ).run()

        self.assertEqual(stdout.getvalue(), '')
        self.assertIn('control_command_unknown', diagnostics.getvalue())

    def test_default_cli_does_not_construct_stdin_controller(self):
        created_rooms = []

        class CliRoom(self.FakeRoom):
            def __init__(self, web_rid, event_sink=None):
                super().__init__()
                self.web_rid = web_rid
                self.event_sink = event_sink
                self.start_calls = 0
                created_rooms.append(self)

            def start(self):
                self.start_calls += 1

        def forbidden_controller(*_args, **_kwargs):
            self.fail('default CLI mode must not construct a stdin controller')

        result = run_collector(
            ['987654'],
            input_stream=io.StringIO('stop\n'),
            output_stream=io.StringIO(),
            diagnostic_stream=io.StringIO(),
            fetcher_factory=CliRoom,
            controller_factory=forbidden_controller,
        )

        self.assertEqual(result, 0)
        self.assertEqual(created_rooms[0].web_rid, '987654')
        self.assertEqual(created_rooms[0].start_calls, 1)
        self.assertEqual(created_rooms[0].stop_calls, 0)

    def test_opt_in_controller_starts_before_blocking_room(self):
        call_order = []

        class CliRoom(self.FakeRoom):
            def __init__(self, _web_rid, event_sink=None):
                super().__init__()

            def start(self):
                call_order.append('room.start')

            def stop(self):
                call_order.append('room.stop')

        class RecordingController:
            def __init__(self, _room, **_kwargs):
                pass

            def start(self):
                call_order.append('control.start')
                return self

            def join(self, timeout):
                call_order.append(('control.join', timeout))

        run_collector(
            ['987654', '--control-stdin'],
            input_stream=io.StringIO(''),
            diagnostic_stream=io.StringIO(),
            fetcher_factory=CliRoom,
            controller_factory=RecordingController,
        )

        self.assertEqual(call_order, [
            'control.start',
            'room.start',
            'room.stop',
            ('control.join', 0.1),
        ])

    def test_real_controller_thread_is_daemon_and_start_is_idempotent(self):
        room = self.FakeRoom()
        controller = StdinStopController(
            room,
            input_stream=io.StringIO('stop\n'),
            diagnostic_stream=io.StringIO(),
        )

        controller.start()
        first_thread = controller._thread
        controller.start()
        controller.join(timeout=1)

        self.assertTrue(first_thread.daemon)
        self.assertIs(controller._thread, first_thread)
        self.assertEqual(room.stop_calls, 1)

    def test_control_reads_always_have_a_finite_bound(self):
        requested_sizes = []

        class RecordingStream:
            def readline(self, size):
                requested_sizes.append(size)
                return 'stop\n'

        room = self.FakeRoom()
        StdinStopController(
            room,
            input_stream=RecordingStream(),
            diagnostic_stream=io.StringIO(),
        ).run()

        self.assertEqual(requested_sizes,
                         [CONTROL_COMMAND_MAX_CHARS + 3])
        self.assertEqual(room.stop_calls, 1)

    def test_join_timeout_does_not_wait_for_blocked_stdin(self):
        release = threading.Event()

        class BlockingStream:
            def readline(self, _size):
                release.wait(timeout=1)
                return ''

        room = self.FakeRoom()
        controller = StdinStopController(
            room,
            input_stream=BlockingStream(),
            diagnostic_stream=io.StringIO(),
        ).start()

        started_at = time.monotonic()
        controller.join(timeout=0.02)
        elapsed = time.monotonic() - started_at

        self.assertLess(elapsed, 0.2)
        release.set()
        controller.join(timeout=1)
        self.assertEqual(room.stop_calls, 1)


if __name__ == '__main__':
    unittest.main()

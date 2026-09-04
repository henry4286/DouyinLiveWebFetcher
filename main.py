#!/usr/bin/python
# coding:utf-8

# @FileName:    main.py
# @Time:        2024/1/2 22:27
# @Author:      bubu
# @Project:     douyinLiveWebFetcher

import argparse
import sys
import threading

from collector_events import NdjsonSink
from liveMan import DouyinLiveWebFetcher


CONTROL_COMMAND_MAX_CHARS = 32


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='Douyin live web event collector')
    parser.add_argument(
        'web_rid',
        nargs='?',
        default='203455341794',
        help='web live room id from https://live.douyin.com/<web_rid>',
    )
    parser.add_argument(
        '--output',
        choices=('human', 'ndjson'),
        default='human',
        help='human keeps legacy logs; ndjson reserves stdout for CollectorEvent v1',
    )
    parser.add_argument(
        '--control-stdin',
        action='store_true',
        help='accept an exact stop command (or EOF) from stdin',
    )
    return parser.parse_args(argv)


def configure_ndjson_stdout(stream=None):
    """Make the CLI NDJSON transport UTF-8 regardless of the host locale."""
    output = stream if stream is not None else sys.stdout
    reconfigure = getattr(output, 'reconfigure', None)
    if reconfigure is not None:
        reconfigure(encoding='utf-8', errors='strict')
    return output


class StdinStopController:
    """Translate the bounded stdin control protocol into an idempotent stop."""

    def __init__(self, room, input_stream=None, diagnostic_stream=None,
                 max_command_chars=CONTROL_COMMAND_MAX_CHARS):
        if max_command_chars < len('stop'):
            raise ValueError('max_command_chars must accommodate stop')
        self._room = room
        self._input = input_stream if input_stream is not None else sys.stdin
        self._diagnostics = (diagnostic_stream if diagnostic_stream is not None
                             else sys.stderr)
        self._max_command_chars = max_command_chars
        self._start_lock = threading.Lock()
        self._thread = None

    def start(self):
        with self._start_lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self.run,
                    name='collector-stdin-control',
                    daemon=True,
                )
                self._thread.start()
        return self

    def join(self, timeout=0.1):
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _diagnose(self, category):
        print(f'[collector-control] {category}',
              file=self._diagnostics, flush=True)

    def _discard_line_tail(self):
        """Drain one rejected line in bounded reads; return True at EOF."""
        while True:
            tail = self._input.readline(self._max_command_chars + 3)
            if tail == '':
                return True
            if tail.endswith('\n'):
                return False

    def _read_command(self):
        """Return (command, category, eof_after_input)."""
        # Three extra characters cover one over-limit character plus CRLF.
        raw = self._input.readline(self._max_command_chars + 3)
        if raw == '':
            return None, None, True

        has_newline = raw.endswith('\n')
        command = raw[:-1] if has_newline else raw
        if has_newline and command.endswith('\r'):
            command = command[:-1]

        if len(command) > self._max_command_chars:
            eof_after_input = (not has_newline and self._discard_line_tail())
            return None, 'control_input_too_long', eof_after_input

        # A bounded readline shorter than its limit and without a newline is
        # the final unterminated command at EOF for the supported text streams.
        return command, None, not has_newline

    def run(self):
        while True:
            try:
                command, category, eof_after_input = self._read_command()
            except Exception:
                self._diagnose('control_read_failure')
                self._room.stop()
                return

            if category is not None:
                self._diagnose(category)
            elif command == 'stop':
                self._room.stop()
                return
            elif command is not None:
                self._diagnose('control_command_unknown')

            if eof_after_input:
                self._room.stop()
                return


def run_collector(argv=None, input_stream=None, output_stream=None,
                  diagnostic_stream=None,
                  fetcher_factory=DouyinLiveWebFetcher,
                  controller_factory=StdinStopController):
    args = parse_args(argv)
    sink = (NdjsonSink(configure_ndjson_stdout(output_stream))
            if args.output == 'ndjson' else None)
    room = fetcher_factory(args.web_rid, event_sink=sink)
    controller = None
    if args.control_stdin:
        controller = controller_factory(
            room,
            input_stream=input_stream,
            diagnostic_stream=diagnostic_stream,
        ).start()

    try:
        room.start()
    finally:
        if controller is not None:
            room.stop()
            controller.join(timeout=0.1)
    return 0

if __name__ == '__main__':
    raise SystemExit(run_collector())

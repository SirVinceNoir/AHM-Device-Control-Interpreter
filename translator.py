#!/usr/bin/env python3
"""
AHM64 ↔ SW42DA Translation Layer
==================================
Bridges the Allen & Heath AHM64 Device Control (TCP) with the Blustream SW42DA (Telnet).

The AHM64 Device Control expects a request/response TCP protocol (boolean model).
The SW42DA uses Telnet and only reports state via its STATUS command rather than
responding to individual control commands.

This service:
  1. Listens for a persistent TCP connection from the AHM64.
  2. Maintains a persistent Telnet connection to the SW42DA.
  3. Translates AHM GET queries into STATUS polls + parsing.
  4. Translates AHM SET commands into the appropriate SW42DA Telnet commands.

AHM Command Protocol (define these strings in AHM Device Control):
  GET commands  → translator queries SW42DA STATUS and returns TRUE or FALSE
  SET commands  → translator sends SW42DA Telnet command and returns OK

Supported GET queries (configure as "Get" string in AHM):
  GET POWER           → TRUE if powered on, FALSE if in power-save
  GET MUTE            → TRUE if muted
  GET INPUT1 .. GET INPUT4   → TRUE if that input is currently selected
  GET OUTPUT1 .. GET OUTPUT3 → TRUE if that output is enabled
  GET ARC             → TRUE if ARC input is selected

Supported SET commands (configure as "Set True" / "Set False" in AHM):
  SET POWER ON / SET POWER OFF
  SET MUTE ON  / SET MUTE OFF
  SET INPUT1   .. SET INPUT4   (select HDMI input)
  SET ARC                      (select ARC input)
  SET OUTPUT1 ON/OFF, SET OUTPUT2 ON/OFF   (HDMI outputs; optical has no enable control)

Expected "Match SetOK" string in AHM: OK
Expected "Match True" string in AHM:  TRUE
Expected "Match False" string in AHM: FALSE
"""

import asyncio
import configparser
import logging
import logging.handlers
import re
import sys
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_FILE = Path(__file__).parent / "config.ini"

_defaults = {
    "ahm_listen_host": "0.0.0.0",
    "ahm_listen_port": "8023",
    "sw42da_host": "192.168.1.100",
    "sw42da_port": "23",
    "sw42da_reconnect_delay": "5",
    "sw42da_command_timeout": "10",
    "status_cache_seconds": "1",
    "log_level": "INFO",
    "log_file": "",
}


def load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser(defaults=_defaults)
    if CONFIG_FILE.exists():
        cfg.read(CONFIG_FILE)
    if not cfg.has_section("translator"):
        cfg.add_section("translator")
    return cfg


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(cfg: configparser.ConfigParser) -> logging.Logger:
    level_name = cfg.get("translator", "log_level", fallback="INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_file = cfg.get("translator", "log_file", fallback="")
    if log_file:
        handlers.append(
            logging.handlers.RotatingFileHandler(
                log_file, maxBytes=5 * 1024 * 1024, backupCount=3
            )
        )
    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    return logging.getLogger("translator")


# ---------------------------------------------------------------------------
# SW42DA Telnet client
# ---------------------------------------------------------------------------

PROMPT = b"SW42DA>"


class SW42DAClient:
    """
    Persistent Telnet client for the Blustream SW42DA.

    Keeps the connection alive and re-establishes it automatically.
    All commands are serialised via an asyncio Lock so concurrent AHM
    polls never interleave their Telnet traffic.
    """

    def __init__(self, host: str, port: int, reconnect_delay: float,
                 command_timeout: float, log: logging.Logger):
        self.host = host
        self.port = port
        self.reconnect_delay = reconnect_delay
        self.command_timeout = command_timeout
        self.log = log
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def _connect(self) -> None:
        self.log.info(f"Connecting to SW42DA at {self.host}:{self.port}…")
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        # Drain the banner / initial prompt
        await self._read_to_prompt(timeout=5.0)
        self.log.info("Connected to SW42DA.")

    async def _read_to_prompt(self, timeout: float = 10.0) -> bytes:
        """Read bytes until the SW42DA prompt is seen or timeout."""
        buf = b""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                chunk = await asyncio.wait_for(
                    self._reader.read(4096), timeout=min(remaining, 1.0)
                )
            except asyncio.TimeoutError:
                break
            if not chunk:
                raise ConnectionResetError("SW42DA closed the connection")
            buf += chunk
            if PROMPT in buf:
                break
        return buf

    async def send_command(self, command: str) -> str:
        """
        Send a command to the SW42DA and return the response text.
        Automatically reconnects if the connection has dropped.
        """
        async with self._lock:
            for attempt in range(3):
                try:
                    if not self.connected:
                        await self._connect()
                    self._writer.write(f"{command}\r\n".encode())
                    await self._writer.drain()
                    raw = await asyncio.wait_for(
                        self._read_to_prompt(timeout=self.command_timeout),
                        timeout=self.command_timeout + 1,
                    )
                    text = raw.decode("utf-8", errors="replace")
                    self.log.debug(f"SW42DA ← {command!r}  →  {text!r}")
                    return text
                except Exception as exc:
                    self.log.warning(
                        f"SW42DA error (attempt {attempt + 1}/3): {exc}"
                    )
                    self._writer = None
                    self._reader = None
                    if attempt < 2:
                        await asyncio.sleep(self.reconnect_delay)
            raise ConnectionError("Could not communicate with SW42DA after 3 attempts")


# ---------------------------------------------------------------------------
# STATUS response parser
# ---------------------------------------------------------------------------
#
# Patterns are matched against the real SW42DA STATUS response format.
# The STATUS output is columnar with a header row followed by a data row.
# Relevant sections (actual device output):
#
#   Power   IR   IR_Mode   Key   Beep   LCD ...
#   On      On   5v        On    Off    On  ...
#
#   Output     FromIn     HDMIcon     OutputEn     OSP     OutputScaler ...
#   01         02         On          Yes          SNK     Bypass       ...
#   02         02         Off         Yes          SNK     Auto         ...
#
#   Master Output     Mute     7.1CH Line Group ...
#   100               Off      On               ...
#
# Note: the SW42DA is a 4×2 matrix switcher — each HDMI output independently
# selects its source.  GET INPUT1–4 queries check output 01's "FromIn" column,
# which is the primary display output.  Output 02 routing is tracked separately
# via GET OUT2IN1–4 if needed (not currently exposed; add controls as required).
#
# Each method returns None if the pattern is not found so the caller can log a
# warning rather than silently returning a wrong value.

class StatusParser:

    # --- Power ---
    # Header: "Power   IR   IR_Mode ..."  →  next line, first token: "On" / "Off"
    _POWER = re.compile(
        r"Power\s+IR\s+IR_Mode[^\n]*\n\s*(\w+)",
        re.IGNORECASE,
    )

    # --- Master mute ---
    # Header: "Master Output     Mute ..."  →  next line: "100   Off ..."
    # Capture the second whitespace-separated token (Mute column).
    _MUTE = re.compile(
        r"Master Output\s+Mute[^\n]*\n\s*\S+\s+(\w+)",
        re.IGNORECASE,
    )

    # --- Current video input routed to output 01 (FromIn column) ---
    # Output table row for output 01: "01   02   On   Yes   ..."
    # The second column (FromIn) is all digits; the Input EDID table rows start
    # with "01  Default_xx" so \d+ safely distinguishes them.
    _INPUT_OUT1 = re.compile(r"^01\s+(\d+)\s", re.MULTILINE)

    # --- Output enabled (OutputEn column: "Yes" / "No") ---
    # Output table row: "01   02   On   Yes   ..."
    # Columns: Output | FromIn | HDMIcon | OutputEn
    # We match on the leading zero-padded output number.
    _OUTPUT_EN = re.compile(
        r"^(0[12])\s+\d+\s+\S+\s+(Yes|No)\b",
        re.MULTILINE | re.IGNORECASE,
    )

    @classmethod
    def power(cls, status: str) -> Optional[bool]:
        m = cls._POWER.search(status)
        return (m.group(1).lower() == "on") if m else None

    @classmethod
    def mute(cls, status: str) -> Optional[bool]:
        m = cls._MUTE.search(status)
        return (m.group(1).lower() == "on") if m else None

    @classmethod
    def input_on_output1(cls, status: str) -> Optional[int]:
        """Return the input number (1–5) currently routed to output 01."""
        m = cls._INPUT_OUT1.search(status)
        return int(m.group(1)) if m else None

    @classmethod
    def output_enabled(cls, status: str, output_num: int) -> Optional[bool]:
        """Return True/False for OutputEn on output 01 or 02."""
        tag = f"0{output_num}"
        for m in cls._OUTPUT_EN.finditer(status):
            if m.group(1) == tag:
                return m.group(2).lower() == "yes"
        return None


# ---------------------------------------------------------------------------
# Command dispatcher
# ---------------------------------------------------------------------------

# SW42DA HDMI output numbers (01 and 02 only — optical is audio-only, no OutputEn in the table).
# The STATUS Output table shows only 01 and 02.
OUTPUT_CMD = {1: "01", 2: "02"}

# SW42DA "OUT FR" input numbers: 01-04 = HDMI inputs 1-4, 05 = ARC input.
INPUT_CMD = {1: "01", 2: "02", 3: "03", 4: "04", "arc": "05"}


class CommandDispatcher:
    """
    Translates the simple AHM command strings into SW42DA Telnet commands
    and returns TRUE / FALSE / OK / ERROR strings back to the AHM.
    """

    def __init__(self, sw42da: SW42DAClient, status_cache_s: float,
                 log: logging.Logger):
        self.sw = sw42da
        self.cache_ttl = status_cache_s
        self.log = log
        self._cached_status: Optional[str] = None
        self._cache_time: float = 0.0
        self._status_lock = asyncio.Lock()
        # Set immediately when a SET INPUT command succeeds so all GET INPUT
        # queries reflect the new state without waiting for the next poll cycle.
        # 0 = no override, 1-4 = HDMI input, 5 = ARC
        self._known_input: int = 0

    async def _status(self) -> str:
        """Return cached STATUS or fetch a fresh one."""
        async with self._status_lock:
            if (self._cached_status is None or
                    time.monotonic() - self._cache_time > self.cache_ttl):
                self._cached_status = await self.sw.send_command("STATUS")
                self._cache_time = time.monotonic()
            return self._cached_status

    def _invalidate_cache(self) -> None:
        self._cached_status = None

    async def handle(self, raw: str) -> str:
        command = raw.strip().upper()
        self.log.info(f"AHM → {command!r}")
        try:
            response = await self._dispatch(command)
        except Exception as exc:
            self.log.error(f"Error handling {command!r}: {exc}")
            response = "ERROR"
        self.log.info(f"AHM ← {response!r}")
        return response

    async def _dispatch(self, command: str) -> str:
        # ------------------------------------------------------------------ GET
        if command.startswith("GET "):
            return await self._get(command[4:].strip())

        # ------------------------------------------------------------------ SET
        if command.startswith("SET "):
            return await self._set(command[4:].strip())

        return "ERROR:UNKNOWN"

    # ------------------------------------------------------------------ GET handlers

    async def _get(self, query: str) -> str:
        status = await self._status()

        # POWER
        if query == "POWER":
            state = StatusParser.power(status)
            if state is None:
                self.log.warning("Could not parse POWER from STATUS response")
                return "ERROR:PARSE"
            return "TRUE" if state else "FALSE"

        # MUTE
        if query == "MUTE":
            state = StatusParser.mute(status)
            if state is None:
                self.log.warning("Could not parse MUTE from STATUS response")
                return "ERROR:PARSE"
            return "TRUE" if state else "FALSE"

        # INPUT1 … INPUT4
        m = re.fullmatch(r"INPUT([1-4])", query)
        if m:
            # Use known_input override if set (populated immediately on SET INPUT)
            if self._known_input:
                return "TRUE" if self._known_input == int(m.group(1)) else "FALSE"
            current = StatusParser.input_on_output1(status)
            if current is not None:
                self._known_input = current  # sync override from STATUS
            if current is None:
                if StatusParser.power(status) is False:
                    return "FALSE"
                self.log.warning("Could not parse FromIn from STATUS response")
                return "FALSE"
            return "TRUE" if current == int(m.group(1)) else "FALSE"

        # ARC (SW42DA FromIn value 05)
        if query == "ARC":
            if self._known_input:
                return "TRUE" if self._known_input == 5 else "FALSE"
            current = StatusParser.input_on_output1(status)
            if current is not None:
                self._known_input = current
            if current is None:
                return "FALSE"
            return "TRUE" if current == 5 else "FALSE"

        # OUTPUT1, OUTPUT2  (HDMI outputs only — optical has no OutputEn in STATUS)
        m = re.fullmatch(r"OUTPUT([12])", query)
        if m:
            state = StatusParser.output_enabled(status, int(m.group(1)))
            if state is None:
                self.log.warning(
                    f"Could not parse OUTPUT{m.group(1)} from STATUS response"
                )
                return "ERROR:PARSE"
            return "TRUE" if state else "FALSE"

        return "ERROR:UNKNOWN_QUERY"

    # ------------------------------------------------------------------ SET handlers

    async def _set(self, command: str) -> str:
        self._invalidate_cache()

        # POWER ON / POWER OFF — delay after to let device stabilise before next STATUS
        if command == "POWER ON":
            await self.sw.send_command("PON")
            await asyncio.sleep(3)
            return "OK"
        if command == "POWER OFF":
            await self.sw.send_command("POFF")
            await asyncio.sleep(2)
            return "OK"

        # MUTE ON / MUTE OFF
        if command == "MUTE ON":
            await self.sw.send_command("MUTE ON")
            return "OK"
        if command == "MUTE OFF":
            await self.sw.send_command("MUTE OFF")
            return "OK"

        # INPUT1 … INPUT4  →  OUT FR 0n
        m = re.fullmatch(r"INPUT([1-4])", command)
        if m:
            n = int(m.group(1))
            await self.sw.send_command(f"OUT FR 0{n}")
            self._known_input = n
            return "OK"

        # ARC  →  OUT FR 05
        if command == "ARC":
            await self.sw.send_command("OUT FR 05")
            self._known_input = 5
            return "OK"

        # OUTPUT1 ON/OFF, OUTPUT2 ON/OFF  (HDMI outputs only)
        m = re.fullmatch(r"OUTPUT([12]) (ON|OFF)", command)
        if m:
            out_num = OUTPUT_CMD[int(m.group(1))]
            await self.sw.send_command(f"OUT {out_num} {m.group(2)}")
            return "OK"

        return "ERROR:UNKNOWN_COMMAND"


# ---------------------------------------------------------------------------
# AHM TCP server
# ---------------------------------------------------------------------------

class AHMServer:
    """
    TCP server that accepts a persistent connection from the AHM64.

    The AHM sends newline-terminated command strings and expects a
    newline-terminated response to each one.

    Multiple simultaneous AHM connections are supported (e.g. if the AHM
    reconnects before the old socket times out), though in practice only
    one connection is expected.
    """

    def __init__(self, dispatcher: CommandDispatcher,
                 host: str, port: int, log: logging.Logger):
        self.dispatcher = dispatcher
        self.host = host
        self.port = port
        self.log = log

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        self.log.info(f"AHM64 connected from {peer}")
        buf = b""
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(reader.read(256), timeout=120.0)
                except asyncio.TimeoutError:
                    self.log.debug("AHM keep-alive timeout — waiting for data")
                    continue

                if not chunk:
                    # EOF — flush any remaining buffered command
                    if buf:
                        command = buf.decode("utf-8", errors="replace").strip()
                        if command:
                            response = await self.dispatcher.handle(command)
                            writer.write(f"{response}\r\n".encode())
                            await writer.drain()
                    break

                buf += chunk
                # Split on \r\n, \r, or \n — AHM may use any of these
                parts = re.split(rb"\r\n|\r|\n", buf)
                buf = parts[-1]  # last part may be incomplete; keep for next read
                for part in parts[:-1]:
                    command = part.decode("utf-8", errors="replace").strip()
                    if not command:
                        # AHM sends empty string for tcpSetFalse on inputs — acknowledge it
                        writer.write(b"OK\r\n")
                        await writer.drain()
                        continue
                    response = await self.dispatcher.handle(command)
                    writer.write(f"{response}\r\n".encode())
                    await writer.drain()

        except ConnectionResetError:
            self.log.info(f"AHM64 connection reset: {peer}")
        except Exception as exc:
            self.log.error(f"Unexpected error on AHM connection {peer}: {exc}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self.log.info(f"AHM64 disconnected: {peer}")

    async def serve_forever(self) -> None:
        server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
        self.log.info(f"Listening for AHM64 on {addrs}")
        async with server:
            await server.serve_forever()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    cfg = load_config()
    log = setup_logging(cfg)

    t = cfg["translator"]
    ahm_host  = t.get("ahm_listen_host")
    ahm_port  = int(t.get("ahm_listen_port"))
    sw_host   = t.get("sw42da_host")
    sw_port   = int(t.get("sw42da_port"))
    reconnect = float(t.get("sw42da_reconnect_delay"))
    timeout   = float(t.get("sw42da_command_timeout"))
    cache_s   = float(t.get("status_cache_seconds"))

    log.info("AHM64 ↔ SW42DA Translator starting…")
    log.info(f"  AHM listen : {ahm_host}:{ahm_port}")
    log.info(f"  SW42DA     : {sw_host}:{sw_port}")

    sw_client  = SW42DAClient(sw_host, sw_port, reconnect, timeout, log)
    dispatcher = CommandDispatcher(sw_client, cache_s, log)
    server     = AHMServer(dispatcher, ahm_host, ahm_port, log)

    # Attempt initial connection; failures are handled gracefully on first command.
    try:
        await sw_client._connect()
    except Exception as exc:
        log.warning(f"Initial SW42DA connection failed ({exc}); will retry on first command.")

    await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

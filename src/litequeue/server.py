import asyncio
import aiosqlite
import os
import yaml
import logging
import click
import uvloop

from typing import Optional
from respparser.parser import RespParser, RespSerializer
from . import logger
from datetime import datetime, timezone

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

OK = "+OK"

# Environment Variables
LITEQUEUE_CONFIG = "LITEQUEUE_CONFIG"
LITEQUEUE_HOST = "LITEQUEUE_HOST"
LITEQUEUE_PORT = "LITEQUEUE_PORT"
LITEQUEUE_VERBOSE = "LITEQUEUE_VERBOSE"


# Queue Statuses
PENDING_STATE = "pending"
PROGRESS_STATE = "progress"
COMPLETE_STATE = "complete"


# QUEUE Configuration
QUEUE_LPUSH_BATCH_MIN_SIZE = 100
QUEUE_LPUSH_BATCH_MAX_SIZE = 100_000
QUEUE_LPUSH_BATCH_MIN_INTERVAL = 0.05
QUEUE_LPUSH_BATCH_MAX_INTERVAL = 5

# Retry Options
DB_RETRY_ATTEMPTS = 100

# TCP Server Settings
TCP_BACKLOG = 100
TCP_LIMIT = 100

# SQLite settings
SQLITE_PRAGMAS = (
    "PRAGMA journal_mode = WAL;",
    "PRAGMA busy_timeout = 10000;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = 1000000000;",
    "PRAGMA foreign_keys = true;",
    "PRAGMA temp_store = memory;",
    "PRAGMA wal_autocheckpoint = 1000;",
)

# Global database connection (initialized once)
db: aiosqlite.Connection = None


def str_now():
    return datetime.now(timezone.utc).strftime("%-Y-%m-%dT%H:%M:%T.%f")


class LiteQueue:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.parser = RespParser()
        self.serializer = RespSerializer()
        self.lpush_buffer = []
        self.batch_size = QUEUE_LPUSH_BATCH_MIN_SIZE  # Commit every X LPUSHes
        self.batch_interval = QUEUE_LPUSH_BATCH_MIN_INTERVAL
        self._lock = asyncio.Lock()
        self._running = True
        self._retry_attempts = DB_RETRY_ATTEMPTS
        # Adaptive batch settings
        self._min_batch_size = QUEUE_LPUSH_BATCH_MIN_SIZE
        self._max_batch_size = QUEUE_LPUSH_BATCH_MAX_SIZE
        self._min_batch_interval = QUEUE_LPUSH_BATCH_MIN_INTERVAL  # seconds
        self._max_batch_interval = QUEUE_LPUSH_BATCH_MAX_INTERVAL  # seconds
        self.batch_size = self._min_batch_size
        self.batch_interval = self._min_batch_interval
        self._request_count = 0  # Track requests for load
        self._last_adjust_time = asyncio.get_event_loop().time()

    async def read_complete_command(self, reader: asyncio.StreamReader) -> bytes:
        line = await reader.readline()
        if not line:
            return b""
        if not line.startswith(b"*"):
            raise ValueError(f"Expected array (*), got: {line.decode().rstrip()}")
        num_elements = int(line[1:].decode().rstrip())
        data = line
        for _ in range(num_elements):
            length_line = await reader.readline()
            data += length_line
            length = int(length_line[1:].decode().rstrip())
            if length != -1:
                value = await reader.readexactly(length + 2)
                data += value
        return data

    async def lpush(self, queue: str, payload: str, commit_now: bool = False) -> bool:
        """Add a task to the left of the queue."""
        task = (queue, payload, PENDING_STATE, str_now())
        async with self._lock:
            self._request_count += 1
            self.lpush_buffer.append(task)
            should_commit = self._lpush_buffer_full() or commit_now
        if should_commit:
            asyncio.create_task(self._commit_lpush_batch())
        return True

    async def _lpush_insert_commit(self, buffer_to_commit) -> None:
        await db.executemany(
            "INSERT INTO tasks (queue, payload, state, created) VALUES (?, ?, ?, ?)",
            buffer_to_commit,
        )
        await db.commit()
        logger.info(f"Committed {len(buffer_to_commit)} LPUSH tasks")

    async def _commit_lpush_batch(self) -> None:
        async with self._lock:
            if not self.lpush_buffer:
                return
            buffer_to_commit = self.lpush_buffer.copy()
            self.lpush_buffer.clear()
        try:
            await self._lpush_insert_commit(buffer_to_commit)
        except aiosqlite.Error as e:
            logger.error(f"SQLite error in LPUSH commit: {e}")
            await db.rollback()
            async with self._lock:
                # Re-add failed batch to front of buffer
                self.lpush_buffer = buffer_to_commit + self.lpush_buffer
            asyncio.create_task(self._commit_lpush_batch())

    def _lpush_buffer_full(self) -> bool:
        return len(self.lpush_buffer) >= self.batch_size

    async def rpop(self, queue: str) -> str | None:
        """Remove and return the task from the right of the queue."""
        # Get the task with the highest position
        cursor = await db.execute(
            "SELECT id, payload FROM tasks WHERE state=? AND queue=? ORDER BY id ASC",
            (
                PENDING_STATE,
                queue,
            ),
        )
        task = await cursor.fetchone()

        if task:
            task_id, payload = task
            # Delete the task
            await db.execute(
                "UPDATE tasks SET state = 'progress' WHERE id = ?", (task_id,)
            )
            await db.commit()
            return payload
        return None

    async def _batch_commit_task(self):
        logger.warning("Started batch_commit")
        while True:
            await asyncio.sleep(self.batch_interval)
            logger.warning("batching from timer")
            await self._commit_lpush_batch()
            # await self._commit_rpop_batch()

    async def _batch_commit_task(self):
        """Run periodic batch commits until stopped."""
        logger.info("Starting batch commit task")
        while self._running:
            try:
                await self._commit_lpush_batch()
                # await self._commit_rpop_batch()
            except Exception as e:
                logger.error(f"Error in batch commit task: {e}")
            await asyncio.sleep(self.batch_interval)
        logger.info("Batch commit task stopped")

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        logger.info(f"New connection {addr}")
        try:
            while True:
                try:
                    data = await self.read_complete_command(reader)
                    if not data:
                        break
                    logger.debug(f"Raw: {data}")
                    command = [cmd.decode() for cmd in self.parser.parse(data)]
                    logger.info(f"Received from {addr}: {command!r}")
                    cmd = command[0].upper() if command else ""
                    if cmd == "LPUSH" and len(command) == 3:
                        queue, payload = command[1], command[2]
                        await self.lpush(queue, payload)
                        writer.write(self.serializer.serialize(OK))
                    elif cmd == "RPOP" and len(command) == 2:
                        queue = command[1]
                        value = await self.rpop(queue)
                        writer.write(self.serializer.serialize(value))
                    else:
                        writer.write(self.serializer.serialize("-ERR invalid command"))
                    await writer.drain()
                except (
                    ConnectionResetError,
                    BrokenPipeError,
                    asyncio.IncompleteReadError,
                ) as e:
                    logger.debug(f"Client {addr} disconnected during operation: {e}")
                    break  # Exit inner loop on client disconnect
                except Exception as e:
                    try:
                        print(f"Error from: {command=}")
                    except Exception:
                        pass
                    logger.error(f"Unexpected error with {addr}: {e}")
                    if not writer.is_closing():
                        writer.write(self.serializer.serialize(f"-ERR {str(e)}"))
                        await writer.drain()
                    break
        finally:
            try:
                if not writer.is_closing():
                    writer.close()
                await writer.wait_closed()
            except Exception:
                logger.debug(f"Connection {addr} already closed by peer")
            logger.info(f"Closed connection from {addr}")

    async def init_db(self):
        """Initialize the database and create the task table."""
        global db
        db = await aiosqlite.connect("tasks.db")
        for pragma in SQLITE_PRAGMAS:
            await db.execute(pragma)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                created TEXT NOT NULL,
                updated TEXT,
                completed TEXT
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_state_queue ON tasks (state, queue)"
        )
        await db.commit()

    async def _adjust_batch_settings(self):
        """Adjust batch size and interval based on load every 10s."""
        while self._running:
            await asyncio.sleep(2)  # Check every 5s
            current_time = asyncio.get_event_loop().time()
            elapsed = current_time - self._last_adjust_time
            if elapsed > 0:
                rate = self._request_count / elapsed  # Requests per second
                self._request_count = 0
                self._last_adjust_time = current_time
                # Linear scaling between min and max based on rate
                # Assume 100 rps = min, 20,000 rps = max
                load_factor = min(max((rate - 100) / (20000 - 100), 0), 1)
                new_batch_size = int(
                    self._min_batch_size
                    + (self._max_batch_size - self._min_batch_size) * load_factor
                )
                new_interval = (
                    self._min_batch_interval
                    + (self._max_batch_interval - self._min_batch_interval)
                    * load_factor
                )
                async with self._lock:
                    self.batch_size = max(
                        self._min_batch_size, min(new_batch_size, self._max_batch_size)
                    )
                    self.batch_interval = max(
                        self._min_batch_interval,
                        min(new_interval, self._max_batch_interval),
                    )
                logger.warning(
                    f"Adjusted batch_size={self.batch_size}, batch_interval={self.batch_interval:.2f}s, rate={rate:.0f} rps"
                )

    async def run(self) -> None:
        await self.init_db()
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port,
            backlog=TCP_BACKLOG,
            limit=TCP_LIMIT,
        )
        addr = server.sockets[0].getsockname()
        logger.info(f"Server running on {addr}")
        # commit_task = asyncio.create_task(self._batch_commit_task())
        adjust_task = asyncio.create_task(self._adjust_batch_settings())
        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("Server shutting down")
            raise
        finally:
            self._running = False
            # Give it a chance to finish one last commit
            await asyncio.sleep(self.batch_interval + 0.1)
            # commit_task.cancel()
            adjust_task.cancel()
            try:
                # await commit_task  # Wait for it to finish
                await adjust_task
            except asyncio.CancelledError:
                logger.debug("Commit task cancelled")
            await self._commit_lpush_batch()
            if db:
                await db.close()
                logger.info("Database connection closed")


def load_config(config_file: str) -> dict:
    """Load configuration from a YAML file, return defaults if not found."""
    defaults = {"host": "0.0.0.0", "port": 6379, "verbose": 0}
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f) or {}
        return {**defaults, **config}
    except (FileNotFoundError, TypeError):  # TypeError if config_file is None
        return defaults


def get_config(
    cli_config: Optional[str],
    cli_host: Optional[str],
    cli_port: Optional[int],
    cli_verbose: Optional[int],
) -> tuple[str, int, int]:
    """Resolve config from CLI, env vars, config file, in that order."""
    # Determine config file path: CLI > Env Var > Default
    config_file = cli_config or os.environ.get(LITEQUEUE_CONFIG) or "config.yaml"
    config = load_config(config_file)

    # Override with environment variables
    env_host = os.environ.get(LITEQUEUE_HOST)
    env_port = os.environ.get(LITEQUEUE_PORT)
    env_verbose = os.environ.get(LITEQUEUE_VERBOSE)

    if env_host:
        config["host"] = env_host
    if env_port:
        config["port"] = int(env_port)
    if env_verbose:
        config["verbose"] = int(env_verbose)

    # Override with CLI args (highest priority)
    host = cli_host if cli_host is not None else config["host"]
    port = cli_port if cli_port is not None else config["port"]
    verbose = cli_verbose if cli_verbose is not None else config["verbose"]

    return host, port, verbose


@click.command()
@click.option("--config", default=None, help="Path to config YAML file")
@click.option("--host", default=None, help="Host to bind to")
@click.option("--port", type=int, default=None, help="Port to listen on")
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Verbosity level (e.g., -v for INFO, -vv for DEBUG)",
)
def start(
    config: Optional[str],
    host: Optional[str],
    port: Optional[int],
    verbose: Optional[int],
) -> None:
    """Start the LiteQueue async TCP server."""
    # Resolve configuration
    host, port, verbose_level = get_config(config, host, port, verbose)

    # Set logging level
    if verbose_level == 0:
        logger.setLevel(logging.WARNING)  # Silent by default
    elif verbose_level == 1:
        logger.setLevel(logging.INFO)
    elif verbose_level >= 2:
        logger.setLevel(logging.DEBUG)

    click.secho(f"Starting LiteQueue on {host}:{port}", fg="green")
    server = LiteQueue(host, port)
    asyncio.run(server.run())


if __name__ == "__main__":
    start()

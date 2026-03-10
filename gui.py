"""
Tkinter GUI for testing teletankerbot - trigger BDTI, Trump, Hormuz manually.
"""
import asyncio
import logging
import queue
import sys
import threading
import tkinter as tk
from tkinter import scrolledtext, ttk

from dotenv import load_dotenv

load_dotenv()

# Import after load_dotenv
from app import fetch_store_and_send, send_telegram
from db import init_db
from marinetraffic import (
    format_snapshot_message,
    get_tanker_snapshot,
    get_vessel_snapshot,
    run_stream,
)
from truth_monitor import check_trump_posts


class QueueHandler(logging.Handler):
    """Log handler that puts records into a queue for the GUI thread."""

    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))


def run_async_loop(loop: asyncio.AbstractEventLoop):
    """Run the asyncio event loop in this thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


class TeletankerbotGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Teletankerbot – Test GUI")
        self.root.geometry("600x450")
        self.root.minsize(400, 300)

        self.loop: asyncio.AbstractEventLoop | None = None
        self.loop_thread: threading.Thread | None = None
        self.vessels: dict = {}
        self.stream_connected = False
        self.stream_task: asyncio.Task | None = None
        self.log_queue: queue.Queue = queue.Queue()

        self._build_ui()
        self._setup_logging()
        self._start_async_loop()
        self._poll_log_queue()
        self._poll_status()

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill=tk.BOTH, expand=True)

        # Buttons
        btn_frame = ttk.Frame(main)
        btn_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(
            btn_frame,
            text="BDTI – Fetch & Send",
            command=self._on_bdti,
        ).pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            btn_frame,
            text="Trump – Check Now",
            command=self._on_trump,
        ).pack(side=tk.LEFT, padx=(0, 8))

        self.hormuz_btn = ttk.Button(
            btn_frame,
            text="Hormuz – Send Snapshot",
            command=self._on_hormuz,
            state=tk.DISABLED,
        )
        self.hormuz_btn.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(
            btn_frame,
            text="Hormuz – Reconnect",
            command=self._on_reconnect,
        ).pack(side=tk.LEFT, padx=(0, 8))

        # Status bar (Hormuz stream + vessel count)
        self.status_var = tk.StringVar(value="Hormuz: connecting… | Vessels: 0")
        status_frame = ttk.Frame(main)
        status_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(status_frame, textvariable=self.status_var, font=("", 9)).pack(anchor=tk.W)

        # Log output
        ttk.Label(main, text="Log:").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(
            main,
            height=20,
            font=("Consolas", 9),
            state=tk.DISABLED,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(4, 0))

    def _setup_logging(self):
        handler = QueueHandler(self.log_queue)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
        )
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    def _start_async_loop(self):
        init_db()
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=run_async_loop, args=(self.loop,), daemon=True)
        self.loop_thread.start()

        # Start Hormuz stream in background
        def set_connected(ok: bool):
            self.stream_connected = ok

        def start_stream():
            if self.stream_task is not None and not self.stream_task.done():
                self.stream_task.cancel()
            self.stream_connected = False
            self.stream_task = asyncio.ensure_future(
                run_stream(self.vessels, connected_callback=set_connected)
            )

        self._start_stream = start_stream
        self.loop.call_soon_threadsafe(start_stream)
        self._log("Hormuz stream starting in background…")

    def _log(self, msg: str):
        self.log_queue.put(msg)

    def _poll_log_queue(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.configure(state=tk.NORMAL)
                self.log_text.insert(tk.END, msg + "\n")
                self.log_text.see(tk.END)
                self.log_text.configure(state=tk.DISABLED)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log_queue)

    def _poll_status(self):
        """Update status bar and Hormuz button state."""
        all_v = get_vessel_snapshot(self.vessels, tankers_only=False)
        tankers = get_tanker_snapshot(self.vessels)
        conn = "connected" if self.stream_connected else "connecting..."
        self.status_var.set(f"Hormuz: {conn} | Vessels: {len(all_v)} (tankers: {len(tankers)})")
        self.hormuz_btn.configure(state=tk.NORMAL if self.stream_connected else tk.DISABLED)
        self.root.after(2000, self._poll_status)

    def _run_coro(self, coro):
        """Schedule coroutine to run in the async loop and log result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)

        def on_done(f):
            try:
                f.result()
                self._log("Done.")
            except Exception as e:
                self._log(f"Error: {e}")

        future.add_done_callback(on_done)

    def _on_bdti(self):
        self._log("Triggering BDTI fetch & send…")
        self._run_coro(fetch_store_and_send())

    def _on_trump(self):
        self._log("Triggering Trump check…")
        self._run_coro(check_trump_posts(send_telegram))

    def _on_reconnect(self):
        self._log("Hormuz: reconnecting…")
        self.loop.call_soon_threadsafe(self._start_stream)

    def _on_hormuz(self):
        if not self.stream_connected:
            self._log("Hormuz: stream not connected – cannot send")
            return
        self._log("Triggering Hormuz snapshot…")

        async def send_hormuz():
            all_v = get_vessel_snapshot(self.vessels, tankers_only=False)
            tankers = get_tanker_snapshot(self.vessels)
            display = all_v if all_v else tankers
            tanker_count = len(tankers) if tankers else None
            msg = format_snapshot_message(display, tanker_count, self.stream_connected)
            ok = await send_telegram(msg)
            self._log(f"Hormuz sent: {len(display)} vessels ({len(tankers)} tankers) – ok={ok}")

        self._run_coro(send_hormuz())

    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    gui = TeletankerbotGUI()
    gui.run()

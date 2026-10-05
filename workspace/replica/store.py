"""灾备复制器持久化层（SQLite，仅标准库）。

存储内容：

========================== ==============================================
meta                       确认边界、分段大小、进程围栏纪元等单行状态
segments                   已确认分段原文（内容寻址：序号 + 摘要唯一）
quarantined                被拒绝/隔离的分段及原因（不污染确认截面）
drills                     受控切换演练及带任期地址（含锁定截面冻结）
audit_log                  只追加审计轨迹（拉取、隔离、演练、任期失效）
========================== ==============================================

确认边界（meta.confirmed_index）与分段写入在**同一事务**提交：
崩溃重启后要么整个分段已确认，要么完全不存在，绝无半截截面。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any

from quota.ledger import canonical

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    confirmed_index  INTEGER NOT NULL DEFAULT 0,
    confirmed_seq    INTEGER NOT NULL DEFAULT 0,
    last_digest      TEXT NOT NULL,
    last_event_hash  TEXT NOT NULL,
    segment_size     INTEGER NOT NULL,
    epoch            INTEGER NOT NULL DEFAULT 1,
    paused           INTEGER NOT NULL DEFAULT 0,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
    segment_index  INTEGER PRIMARY KEY,
    seq_start      INTEGER NOT NULL,
    seq_end        INTEGER NOT NULL,
    prev_digest    TEXT NOT NULL,
    digest         TEXT NOT NULL UNIQUE,
    envelope_json  TEXT NOT NULL,
    applied_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantined (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_index    INTEGER,
    reason           TEXT NOT NULL,
    detail           TEXT NOT NULL,
    source           TEXT NOT NULL,
    envelope_digest  TEXT,
    epoch            INTEGER,
    detected_at      REAL NOT NULL,
    resolved_at      REAL,
    resolution       TEXT
);

CREATE TABLE IF NOT EXISTS drills (
    drill_id         TEXT PRIMARY KEY,
    state            TEXT NOT NULL,
    locked_index     INTEGER,
    locked_digest    TEXT,
    locked_event_hash TEXT,
    term_token       TEXT,
    term_expires_at  REAL,
    max_lag_segments INTEGER NOT NULL,
    report_json      TEXT NOT NULL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    finished_at      REAL,
    fail_reason      TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    kind        TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""

GENESIS_HASH = "0" * 64

# 演练状态机：活动状态（可迁往终态）与终态
DRILL_ACTIVE_STATES = ("VALIDATING", "ISSUED")
DRILL_TERMINAL_STATES = ("CANCELLED", "ABORTED", "EXPIRED")


def new_token() -> str:
    return "drill-" + uuid.uuid4().hex


class ReplicaStore:
    def __init__(self, path: str, segment_size: int):
        self.path = path
        first_time = not os.path.exists(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        if first_time:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO meta (id, confirmed_index, confirmed_seq,"
                    " last_digest, last_event_hash, segment_size, epoch, paused, updated_at)"
                    " VALUES (1, 0, 0, ?, ?, ?, 1, 0, ?)",
                    (GENESIS_HASH, GENESIS_HASH, segment_size, time.time()),
                )

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------
    # meta
    # ------------------------------------------------------------------

    def meta(self) -> dict[str, Any]:
        with self.lock:
            r = self.conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        return dict(r)

    def get_epoch(self) -> int:
        return int(self.meta()["epoch"])

    def bump_epoch(self) -> int:
        """新进程接管：纪元 +1。旧纪元进程的迟到写入将被拒绝。"""
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE meta SET epoch = epoch + 1, updated_at = ? WHERE id = 1",
                (time.time(),),
            )
        return self.get_epoch()

    def set_paused(self, paused: bool) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE meta SET paused = ?, updated_at = ? WHERE id = 1",
                (1 if paused else 0, time.time()),
            )

    # ------------------------------------------------------------------
    # segments
    # ------------------------------------------------------------------

    def commit_segment(
        self, env: dict[str, Any], last_event_hash: str
    ) -> None:
        """分段确认 + 边界推进，单事务原子提交。"""
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO segments (segment_index, seq_start, seq_end, prev_digest,"
                " digest, envelope_json, applied_at) VALUES (?,?,?,?,?,?,?)",
                (
                    env["segment_index"],
                    env["seq_start"],
                    env["seq_end"],
                    env["prev_digest"],
                    env["digest"],
                    canonical(env),
                    time.time(),
                ),
            )
            self.conn.execute(
                "UPDATE meta SET confirmed_index = ?, confirmed_seq = ?,"
                " last_digest = ?, last_event_hash = ?, updated_at = ? WHERE id = 1",
                (
                    env["segment_index"],
                    env["seq_end"],
                    env["digest"],
                    last_event_hash,
                    time.time(),
                ),
            )

    def load_segments(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT envelope_json FROM segments ORDER BY segment_index"
            ).fetchall()
        return [json.loads(r["envelope_json"]) for r in rows]

    def get_segment(self, index: int) -> dict[str, Any] | None:
        with self.lock:
            r = self.conn.execute(
                "SELECT envelope_json FROM segments WHERE segment_index = ?", (index,)
            ).fetchone()
        return json.loads(r["envelope_json"]) if r else None

    def segment_digests(self) -> dict[int, str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT segment_index, digest FROM segments ORDER BY segment_index"
            ).fetchall()
        return {int(r["segment_index"]): r["digest"] for r in rows}

    # ------------------------------------------------------------------
    # quarantined
    # ------------------------------------------------------------------

    def quarantine(
        self,
        reason: str,
        detail: str,
        source: str,
        segment_index: int | None = None,
        envelope_digest_value: str | None = None,
        epoch: int | None = None,
    ) -> int:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "INSERT INTO quarantined (segment_index, reason, detail, source,"
                " envelope_digest, epoch, detected_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (
                    segment_index,
                    reason,
                    detail,
                    source,
                    envelope_digest_value,
                    epoch,
                    time.time(),
                ),
            )
            return int(cur.lastrowid)

    def list_quarantined(self, include_resolved: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM quarantined"
        if not include_resolved:
            sql += " WHERE resolved_at IS NULL"
        sql += " ORDER BY id DESC LIMIT 200"
        with self.lock:
            rows = self.conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def resolve_open_for_index(self, segment_index: int, resolution: str) -> None:
        """该序号分段已被健康分段确认：此前对它的隔离记录标记为已消除。"""
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE quarantined SET resolved_at = ?, resolution = ?"
                " WHERE segment_index = ? AND resolved_at IS NULL",
                (time.time(), resolution, segment_index),
            )

    def has_open_gap(self) -> bool:
        with self.lock:
            r = self.conn.execute(
                "SELECT 1 FROM quarantined WHERE resolved_at IS NULL LIMIT 1"
            ).fetchone()
        return r is not None

    # ------------------------------------------------------------------
    # drills
    # ------------------------------------------------------------------

    def upsert_drill(self, drill: dict[str, Any]) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO drills (drill_id, state, locked_index, locked_digest,"
                " locked_event_hash, term_token, term_expires_at, max_lag_segments,"
                " report_json, created_at, updated_at, finished_at, fail_reason)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(drill_id) DO UPDATE SET state=excluded.state,"
                " locked_index=excluded.locked_index, locked_digest=excluded.locked_digest,"
                " locked_event_hash=excluded.locked_event_hash,"
                " term_token=excluded.term_token, term_expires_at=excluded.term_expires_at,"
                " report_json=excluded.report_json, updated_at=excluded.updated_at,"
                " finished_at=excluded.finished_at, fail_reason=excluded.fail_reason",
                (
                    drill["drill_id"],
                    drill["state"],
                    drill.get("locked_index"),
                    drill.get("locked_digest"),
                    drill.get("locked_event_hash"),
                    drill.get("term_token"),
                    drill.get("term_expires_at"),
                    drill["max_lag_segments"],
                    canonical(drill.get("report") or {}),
                    drill["created_at"],
                    drill["updated_at"],
                    drill.get("finished_at"),
                    drill.get("fail_reason"),
                ),
            )

    @staticmethod
    def _row_to_drill(r: sqlite3.Row | None) -> dict[str, Any] | None:
        if r is None:
            return None
        d = dict(r)
        d["report"] = json.loads(d["report_json"]) if d["report_json"] else {}
        del d["report_json"]
        return d

    def get_drill(self, drill_id: str) -> dict[str, Any] | None:
        with self.lock:
            r = self.conn.execute(
                "SELECT * FROM drills WHERE drill_id = ?", (drill_id,)
            ).fetchone()
        return self._row_to_drill(r)

    def list_drills(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM drills ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [d for d in (self._row_to_drill(r) for r in rows) if d is not None]

    def active_drill(self) -> dict[str, Any] | None:
        with self.lock:
            r = self.conn.execute(
                "SELECT * FROM drills WHERE state IN ('VALIDATING','ISSUED')"
                " ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return self._row_to_drill(r)

    def drill_by_token(self, token: str) -> dict[str, Any] | None:
        with self.lock:
            r = self.conn.execute(
                "SELECT * FROM drills WHERE term_token = ?", (token,)
            ).fetchone()
        return self._row_to_drill(r)

    def finish_drill(
        self,
        drill_id: str,
        *,
        state: str,
        reason: str,
        detail: str,
        report: dict[str, Any],
        finished_at: float,
    ) -> bool:
        """把演练原子地落入终态（CAS）并同事务写下唯一一条终态审计。

        仅当当前仍处于活动状态（VALIDATING/ISSUED）时迁移成功、返回 True；
        并发/跨进程（取消、到期清扫、重启作废、迟到读取）的另一个终态迁移
        会落败并返回 False —— 无论多少路径相撞，只落下一次终态、一条审计。
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE drills SET state=?, term_token=NULL, term_expires_at=NULL,"
                " report_json=?, updated_at=?, finished_at=?, fail_reason=?"
                f" WHERE drill_id=? AND state IN ({','.join('?' * len(DRILL_ACTIVE_STATES))})",
                (state, canonical(report), finished_at, finished_at, reason,
                 drill_id, *DRILL_ACTIVE_STATES),
            )
            if cur.rowcount == 0:
                return False
            self.conn.execute(
                "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
                (
                    finished_at,
                    "drill_finished",
                    canonical({
                        "drill_id": drill_id, "state": state,
                        "reason": reason, "detail": detail,
                    }),
                ),
            )
        return True

    # ------------------------------------------------------------------
    # audit
    # ------------------------------------------------------------------

    def audit(self, kind: str, detail: dict[str, Any]) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO audit_log (at, kind, detail_json) VALUES (?,?,?)",
                (time.time(), kind, canonical(detail)),
            )

    def list_audit(self, limit: int = 100, kind: str | None = None) -> list[dict[str, Any]]:
        if kind:
            sql = ("SELECT id, at, kind, detail_json FROM audit_log"
                   " WHERE kind = ? ORDER BY id DESC LIMIT ?")
            args: tuple = (kind, limit)
        else:
            sql = ("SELECT id, at, kind, detail_json FROM audit_log"
                   " ORDER BY id DESC LIMIT ?")
            args = (limit,)
        with self.lock:
            rows = self.conn.execute(sql, args).fetchall()
        return [
            {"id": r["id"], "at": r["at"], "kind": r["kind"],
             "detail": json.loads(r["detail_json"])}
            for r in rows
        ]

    def close(self) -> None:
        with self.lock:
            self.conn.close()

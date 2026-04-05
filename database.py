import json
import os
import random
import sqlite3
import string
from contextlib import contextmanager
from pathlib import Path

from settlement import minimize_settlements

DB_PATH = os.environ.get("DATABASE_PATH", str(Path(__file__).resolve().parent / "tripsplit.db"))


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS groups (
                id TEXT PRIMARY KEY,
                leader TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                name TEXT NOT NULL,
                upi_id TEXT,
                UNIQUE(group_id, name),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                payer_id INTEGER NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('PAID', 'PENDING')),
                participant_ids TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                FOREIGN KEY (payer_id) REFERENCES members(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS settlements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                from_user INTEGER NOT NULL,
                to_user INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('PENDING', 'SETTLED')),
                payment_type TEXT NOT NULL CHECK(payment_type IN ('UPI', 'CASH')),
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                FOREIGN KEY (from_user) REFERENCES members(id) ON DELETE CASCADE,
                FOREIGN KEY (to_user) REFERENCES members(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_members_group ON members(group_id);
            CREATE INDEX IF NOT EXISTS idx_expenses_group ON expenses(group_id);
            CREATE INDEX IF NOT EXISTS idx_settlements_group ON settlements(group_id);
            """
        )


def generate_group_id(conn, length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    for _ in range(200):
        gid = "".join(random.choices(chars, k=length))
        row = conn.execute("SELECT 1 FROM groups WHERE id = ?", (gid,)).fetchone()
        if not row:
            return gid
    raise RuntimeError("Could not allocate group id")


def create_group(leader_name: str) -> tuple[str, int]:
    with get_db() as conn:
        gid = generate_group_id(conn)
        conn.execute("INSERT INTO groups (id, leader) VALUES (?, ?)", (gid, leader_name))
        cur = conn.execute(
            "INSERT INTO members (group_id, name) VALUES (?, ?)", (gid, leader_name)
        )
        return gid, cur.lastrowid


def get_group(gid: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM groups WHERE id = ?", (gid,)).fetchone()
        return dict(row) if row else None


def list_members(conn, gid: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM members WHERE group_id = ? ORDER BY id",
        (gid,),
    ).fetchall()


def add_member(gid: str, name: str, upi_id: str | None = None) -> tuple[bool, str, int | None]:
    name = (name or "").strip()
    if not name:
        return False, "Name cannot be empty", None
    with get_db() as conn:
        if not conn.execute("SELECT 1 FROM groups WHERE id = ?", (gid,)).fetchone():
            return False, "Group not found", None
        row = conn.execute(
            "SELECT 1 FROM members WHERE group_id = ? AND LOWER(name) = LOWER(?)",
            (gid, name),
        ).fetchone()
        if row:
            return False, "That name is already in this group", None
        cur = conn.execute(
            "INSERT INTO members (group_id, name, upi_id) VALUES (?, ?, ?)",
            (gid, name, (upi_id or "").strip() or None),
        )
        return True, "ok", cur.lastrowid


def update_member_upi(gid: str, member_id: int, upi_id: str) -> bool:
    with get_db() as conn:
        r = conn.execute(
            "UPDATE members SET upi_id = ? WHERE id = ? AND group_id = ?",
            ((upi_id or "").strip() or None, member_id, gid),
        )
        return r.rowcount > 0


def get_expense_participant_ids(conn, gid: str) -> list[int]:
    rows = list_members(conn, gid)
    return [r["id"] for r in rows]


def add_expense(
    gid: str,
    payer_id: int,
    description: str,
    amount: float,
    status: str,
    participant_ids: list[int] | None = None,
) -> tuple[bool, str, int | None]:
    if status not in ("PAID", "PENDING"):
        return False, "Invalid status", None
    description = (description or "").strip()
    if not description:
        return False, "Description required", None
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return False, "Invalid amount", None
    if amount <= 0:
        return False, "Amount must be positive", None

    with get_db() as conn:
        g = conn.execute("SELECT 1 FROM groups WHERE id = ?", (gid,)).fetchone()
        if not g:
            return False, "Group not found", None
        payer = conn.execute(
            "SELECT id FROM members WHERE id = ? AND group_id = ?",
            (payer_id, gid),
        ).fetchone()
        if not payer:
            return False, "Invalid payer", None

        if participant_ids is None:
            participant_ids = get_expense_participant_ids(conn, gid)
        if len(participant_ids) < 1:
            return False, "Add at least one member to split", None
        for pid in participant_ids:
            m = conn.execute(
                "SELECT id FROM members WHERE id = ? AND group_id = ?",
                (pid, gid),
            ).fetchone()
            if not m:
                return False, "Invalid split member", None

        pid_json = json.dumps(participant_ids)
        cur = conn.execute(
            """
            INSERT INTO expenses (group_id, payer_id, description, amount, status, participant_ids)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (gid, payer_id, description, amount, status, pid_json),
        )
        return True, "ok", cur.lastrowid


def list_expenses(conn, gid: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT e.*, m.name AS payer_name
        FROM expenses e
        JOIN members m ON m.id = e.payer_id
        WHERE e.group_id = ?
        ORDER BY e.id DESC
        """,
        (gid,),
    ).fetchall()


def list_settlements(conn, gid: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT s.*, mf.name AS from_name, mt.name AS to_name
        FROM settlements s
        JOIN members mf ON mf.id = s.from_user
        JOIN members mt ON mt.id = s.to_user
        WHERE s.group_id = ?
        ORDER BY s.id DESC
        """,
        (gid,),
    ).fetchall()


def insert_settlement(
    gid: str, from_id: int, to_id: int, amount: float, status: str, payment_type: str
) -> int | None:
    if status not in ("PENDING", "SETTLED") or payment_type not in ("UPI", "CASH"):
        return None
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO settlements (group_id, from_user, to_user, amount, status, payment_type)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (gid, from_id, to_id, amount, status, payment_type),
        )
        return cur.lastrowid


def update_settlement_status(sid: int, gid: str, status: str) -> bool:
    if status not in ("PENDING", "SETTLED"):
        return False
    with get_db() as conn:
        r = conn.execute(
            "UPDATE settlements SET status = ? WHERE id = ? AND group_id = ?",
            (status, sid, gid),
        )
        return r.rowcount > 0


def compute_balances_and_suggestions(conn, gid: str):
    """Returns (per_member_stats, net_after_settlements, suggested_transfers)."""
    members = list_members(conn, gid)
    mids = [m["id"] for m in members]
    if not mids:
        return {}, {}, []

    paid_total: dict[int, float] = {m: 0.0 for m in mids}
    share_total: dict[int, float] = {m: 0.0 for m in mids}

    expenses = conn.execute(
        "SELECT * FROM expenses WHERE group_id = ?",
        (gid,),
    ).fetchall()
    for ex in expenses:
        try:
            parts = json.loads(ex["participant_ids"])
        except json.JSONDecodeError:
            parts = mids
        parts = [int(p) for p in parts if int(p) in share_total]
        if not parts:
            continue
        share = float(ex["amount"]) / len(parts)
        payer_id = int(ex["payer_id"])
        if ex["status"] == "PAID" and payer_id in paid_total:
            paid_total[payer_id] += float(ex["amount"])
        for pid in parts:
            if pid in share_total:
                share_total[pid] += share

    settlements = conn.execute(
        "SELECT * FROM settlements WHERE group_id = ? AND status = 'SETTLED'",
        (gid,),
    ).fetchall()
    net: dict[int, float] = {}
    for m in mids:
        net[m] = paid_total.get(m, 0) - share_total.get(m, 0)

    for s in settlements:
        fid, tid = int(s["from_user"]), int(s["to_user"])
        amt = float(s["amount"])
        if fid in net:
            net[fid] += amt
        if tid in net:
            net[tid] -= amt

    suggested = minimize_settlements(net)

    per_member = {}
    for m in members:
        mid = m["id"]
        per_member[mid] = {
            "id": mid,
            "name": m["name"],
            "paid_total": round(paid_total.get(mid, 0), 2),
            "share_total": round(share_total.get(mid, 0), 2),
            "balance": round(net.get(mid, 0), 2),
        }

    return per_member, net, suggested

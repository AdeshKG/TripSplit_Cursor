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


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(r["name"] == column_name for r in rows)


def init_db():
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS groups (
                id TEXT PRIMARY KEY,
                name TEXT,
                leader TEXT NOT NULL,
                last_split_paid_expense_id INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                name TEXT NOT NULL,
                upi_id TEXT NOT NULL,
                UNIQUE(group_id, name),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                payer_id INTEGER NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                split_type TEXT NOT NULL DEFAULT 'EQUAL',
                status TEXT NOT NULL CHECK(status IN ('PAID', 'PENDING')),
                participant_ids TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                FOREIGN KEY (payer_id) REFERENCES members(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                sender_id INTEGER NOT NULL,
                receiver_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('PENDING', 'CLAIMED_PAID', 'SETTLED')),
                payment_type TEXT NOT NULL CHECK(payment_type IN ('UPI', 'CASH')),
                transaction_ref TEXT,
                proof_image TEXT,
                claimed_at TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                FOREIGN KEY (sender_id) REFERENCES members(id) ON DELETE CASCADE,
                FOREIGN KEY (receiver_id) REFERENCES members(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                member_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                message TEXT NOT NULL,
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                FOREIGN KEY (member_id) REFERENCES members(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_members_group ON members(group_id);
            CREATE INDEX IF NOT EXISTS idx_expenses_group ON expenses(group_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_group ON transactions(group_id);
            CREATE INDEX IF NOT EXISTS idx_notifications_group_member ON notifications(group_id, member_id);
            """
        )

        if not _column_exists(conn, "groups", "name"):
            conn.execute("ALTER TABLE groups ADD COLUMN name TEXT")
        if not _column_exists(conn, "groups", "last_split_paid_expense_id"):
            conn.execute(
                "ALTER TABLE groups ADD COLUMN last_split_paid_expense_id INTEGER NOT NULL DEFAULT 0"
            )
        if not _column_exists(conn, "expenses", "split_type"):
            conn.execute("ALTER TABLE expenses ADD COLUMN split_type TEXT NOT NULL DEFAULT 'EQUAL'")
        if _column_exists(conn, "members", "upi_id"):
            conn.execute("UPDATE members SET upi_id = COALESCE(upi_id, '')")


def generate_group_id(conn, length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    for _ in range(200):
        gid = "".join(random.choices(chars, k=length))
        row = conn.execute("SELECT 1 FROM groups WHERE id = ?", (gid,)).fetchone()
        if not row:
            return gid
    raise RuntimeError("Could not allocate group id")


def create_group(group_name: str, leader_name: str, leader_upi: str) -> tuple[str, int]:
    group_name = (group_name or "").strip()
    leader_name = (leader_name or "").strip()
    leader_upi = (leader_upi or "").strip()
    with get_db() as conn:
        gid = generate_group_id(conn)
        conn.execute(
            "INSERT INTO groups (id, name, leader) VALUES (?, ?, ?)",
            (gid, group_name, leader_name),
        )
        cur = conn.execute(
            "INSERT INTO members (group_id, name, upi_id) VALUES (?, ?, ?)",
            (gid, leader_name, leader_upi),
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
    upi_id = (upi_id or "").strip()
    if not name:
        return False, "Name cannot be empty", None
    if not upi_id:
        return False, "UPI ID or UPI-linked phone is required", None
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
            (gid, name, upi_id),
        )
        return True, "ok", cur.lastrowid


def update_member_upi(gid: str, member_id: int, upi_id: str) -> bool:
    upi_id = (upi_id or "").strip()
    if not upi_id:
        return False
    with get_db() as conn:
        r = conn.execute(
            "UPDATE members SET upi_id = ? WHERE id = ? AND group_id = ?",
            (upi_id, member_id, gid),
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
    split_type: str = "EQUAL",
) -> tuple[bool, str, int | None]:
    if status not in ("PAID", "PENDING"):
        return False, "Invalid status", None
    if split_type not in ("EQUAL", "CUSTOM"):
        return False, "Invalid split_type", None
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
            INSERT INTO expenses (group_id, payer_id, description, amount, split_type, status, participant_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (gid, payer_id, description, amount, split_type, status, pid_json),
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


def list_transactions(conn, gid: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT t.*, ms.name AS sender_name, mr.name AS receiver_name
        FROM transactions t
        JOIN members ms ON ms.id = t.sender_id
        JOIN members mr ON mr.id = t.receiver_id
        WHERE t.group_id = ?
        ORDER BY t.id DESC
        """,
        (gid,),
    ).fetchall()


def create_transaction(
    gid: str,
    sender_id: int,
    receiver_id: int,
    amount: float,
    payment_type: str,
    transaction_ref: str | None = None,
    proof_image: str | None = None,
) -> tuple[bool, str, int | None]:
    if payment_type not in ("UPI", "CASH"):
        return False, "Invalid payment type", None
    if amount <= 0:
        return False, "Amount must be positive", None
    with get_db() as conn:
        sid = conn.execute(
            "SELECT 1 FROM members WHERE id = ? AND group_id = ?",
            (sender_id, gid),
        ).fetchone()
        rid = conn.execute(
            "SELECT 1 FROM members WHERE id = ? AND group_id = ?",
            (receiver_id, gid),
        ).fetchone()
        if not sid or not rid or sender_id == receiver_id:
            return False, "Invalid sender/receiver", None
        cur = conn.execute(
            """
            INSERT INTO transactions (
                group_id, sender_id, receiver_id, amount, status, payment_type, transaction_ref, proof_image
            ) VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?)
            """,
            (gid, sender_id, receiver_id, amount, payment_type, transaction_ref, proof_image),
        )
        return True, "ok", cur.lastrowid


def claim_transaction_paid(
    gid: str, transaction_id: int, sender_id: int, transaction_ref: str | None = None, proof_image: str | None = None
) -> bool:
    with get_db() as conn:
        r = conn.execute(
            """
            UPDATE transactions
            SET status = 'CLAIMED_PAID',
                claimed_at = datetime('now'),
                updated_at = datetime('now'),
                transaction_ref = COALESCE(?, transaction_ref),
                proof_image = COALESCE(?, proof_image)
            WHERE id = ? AND group_id = ? AND sender_id = ? AND status = 'PENDING'
            """,
            (transaction_ref, proof_image, transaction_id, gid, sender_id),
        )
        return r.rowcount > 0


def confirm_transaction(
    gid: str, transaction_id: int, receiver_id: int, received: bool
) -> bool:
    next_status = "SETTLED" if received else "PENDING"
    with get_db() as conn:
        r = conn.execute(
            """
            UPDATE transactions
            SET status = ?, updated_at = datetime('now')
            WHERE id = ? AND group_id = ? AND receiver_id = ? AND status = 'CLAIMED_PAID'
            """,
            (next_status, transaction_id, gid, receiver_id),
        )
        return r.rowcount > 0


def create_notification(gid: str, member_id: int, kind: str, message: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO notifications (group_id, member_id, kind, message) VALUES (?, ?, ?, ?)",
            (gid, member_id, kind, message),
        )


def list_notifications(conn, gid: str, member_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM notifications
        WHERE group_id = ? AND member_id = ?
        ORDER BY id DESC
        LIMIT 30
        """,
        (gid, member_id),
    ).fetchall()


def compute_balances_and_suggestions(conn, gid: str):
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

    txns = conn.execute(
        "SELECT * FROM transactions WHERE group_id = ? AND status = 'SETTLED'",
        (gid,),
    ).fetchall()
    net: dict[int, float] = {}
    for m in mids:
        net[m] = paid_total.get(m, 0) - share_total.get(m, 0)

    for t in txns:
        fid, tid = int(t["sender_id"]), int(t["receiver_id"])
        amt = float(t["amount"])
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


def generate_final_split_transactions(gid: str) -> tuple[bool, str, int]:
    with get_db() as conn:
        group = conn.execute(
            "SELECT id, last_split_paid_expense_id FROM groups WHERE id = ?",
            (gid,),
        ).fetchone()
        if not group:
            return False, "Group not found", 0

        open_tx = conn.execute(
            "SELECT 1 FROM transactions WHERE group_id = ? AND status IN ('PENDING', 'CLAIMED_PAID') LIMIT 1",
            (gid,),
        ).fetchone()
        if open_tx:
            return False, "Settle existing pending transactions before final split", 0

        latest_paid = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id FROM expenses WHERE group_id = ? AND status = 'PAID'",
            (gid,),
        ).fetchone()["max_id"]
        if int(latest_paid) == 0:
            return False, "No PAID expenses to split", 0
        if int(latest_paid) <= int(group["last_split_paid_expense_id"]):
            return False, "Split already generated. Add new PAID expense to split again.", 0

        members = list_members(conn, gid)
        if not members:
            return False, "No members in group", 0
        member_ids = [int(m["id"]) for m in members]

        paid_rows = conn.execute(
            "SELECT payer_id, amount FROM expenses WHERE group_id = ? AND status = 'PAID'",
            (gid,),
        ).fetchall()
        total = sum(float(r["amount"]) for r in paid_rows)
        share = total / len(member_ids)

        net = {mid: -share for mid in member_ids}
        for r in paid_rows:
            net[int(r["payer_id"])] += float(r["amount"])

        suggestions = minimize_settlements(net)
        created = 0
        for from_id, to_id, amount in suggestions:
            if amount <= 0.0:
                continue
            conn.execute(
                """
                INSERT INTO transactions (group_id, sender_id, receiver_id, amount, status, payment_type)
                VALUES (?, ?, ?, ?, 'PENDING', 'UPI')
                """,
                (gid, int(from_id), int(to_id), float(amount)),
            )
            created += 1

        conn.execute(
            "UPDATE groups SET last_split_paid_expense_id = ? WHERE id = ?",
            (int(latest_paid), gid),
        )
        return True, "ok", created

import os
import re
from urllib.parse import quote

from flask import Flask, jsonify, render_template, request, session, url_for

import database as db

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-tripsplit-change-me")


def upi_deep_link(payee_upi: str, payee_name: str, amount: float, note: str) -> str:
    pa = (payee_upi or "").strip()
    if " " in pa:
        pa = pa.replace(" ", "")
    pn = quote((payee_name or "Payee").strip(), safe=" ")
    am = f"{float(amount):.2f}"
    tn = quote((note or "Splitzy payment")[:80], safe=" ")
    return f"upi://pay?pa={pa}&pn={pn}&am={am}&cu=INR&tn={tn}"


def _json_error(message: str, code: int = 400):
    return jsonify({"ok": False, "error": message}), code


def _is_valid_upi_or_phone(value: str) -> bool:
    v = (value or "").strip()
    upi_re = re.compile(r"^[A-Za-z0-9._-]{2,}@[A-Za-z]{2,}$")
    phone_re = re.compile(r"^[6-9][0-9]{9}$")
    return bool(upi_re.match(v) or phone_re.match(v))


def _is_upi_id(value: str) -> bool:
    return bool(re.compile(r"^[A-Za-z0-9._-]{2,}@[A-Za-z]{2,}$").match((value or "").strip()))


def _session_key(gid: str) -> str:
    return f"current_user_{gid}"


def _current_user_id(gid: str) -> int | None:
    raw = session.get(_session_key(gid))
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _group_state(gid: str):
    with db.get_db() as conn:
        group = conn.execute("SELECT * FROM groups WHERE id = ?", (gid,)).fetchone()
        if not group:
            return None
        members = db.list_members(conn, gid)
        expenses = db.list_expenses(conn, gid)
        settlements = db.list_transactions(conn, gid)
        per_member, net, suggested = db.compute_balances_and_suggestions(conn, gid)
        notifications = []
        current_user_id = _current_user_id(gid)
        if current_user_id:
            notifications = [dict(n) for n in db.list_notifications(conn, gid, current_user_id)]

    member_by_id = {m["id"]: dict(m) for m in members}
    sug_named = []
    for fid, tid, amt in suggested:
        sug_named.append(
            {
                "from_id": fid,
                "to_id": tid,
                "amount": amt,
                "from_name": member_by_id.get(fid, {}).get("name", "?"),
                "to_name": member_by_id.get(tid, {}).get("name", "?"),
                "to_upi": member_by_id.get(tid, {}).get("upi_id") or "",
            }
        )

    return {
        "group": dict(group),
        "members": [dict(m) for m in members],
        "expenses": [dict(e) for e in expenses],
        "settlements": [dict(s) for s in settlements],
        "balances": list(per_member.values()),
        "suggested_settlements": sug_named,
        "notifications": notifications,
        "current_user_id": current_user_id,
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/group/<gid>")
def group_page(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return render_template("not_found.html", gid=gid), 404
    return render_template("group.html", gid=gid)


@app.route("/group/<gid>/expense/pay")
def expense_pay_page(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return render_template("not_found.html", gid=gid), 404
    return render_template("expense_pay.html", gid=gid)


@app.route("/group/<gid>/settle/pay")
def settle_pay_page(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return render_template("not_found.html", gid=gid), 404
    return render_template("settle_pay.html", gid=gid)


@app.post("/api/groups")
def api_create_group():
    data = request.get_json(silent=True) or {}
    group_name = (data.get("name") or "").strip()
    leader = (data.get("leader") or "").strip()
    leader_upi = (data.get("leader_upi") or "").strip()
    if not group_name:
        return _json_error("Group name is required")
    if not leader:
        return _json_error("Leader name is required")
    if not leader_upi:
        return _json_error("Leader UPI ID or UPI-linked phone is required")
    if not _is_valid_upi_or_phone(leader_upi):
        return _json_error("Enter valid UPI ID or 10-digit phone number")
    gid, _ = db.create_group(group_name, leader, leader_upi)
    return jsonify({"ok": True, "group_id": gid, "redirect": url_for("group_page", gid=gid)})


@app.get("/api/groups/<gid>")
def api_group_state(gid):
    gid = gid.strip().upper()
    state = _group_state(gid)
    if not state:
        return _json_error("Group not found", 404)
    return jsonify({"ok": True, **state})


@app.post("/api/groups/<gid>/members")
def api_add_member(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    if not _is_valid_upi_or_phone(data.get("upi_id") or ""):
        return _json_error("Enter valid UPI ID or 10-digit phone number")
    ok, msg, mid = db.add_member(gid, data.get("name"), data.get("upi_id"))
    if not ok:
        return _json_error(msg)
    return jsonify({"ok": True, "member_id": mid})


@app.patch("/api/groups/<gid>/members/<int:mid>")
def api_patch_member(gid, mid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    if "upi_id" in data:
        if not _is_valid_upi_or_phone(data.get("upi_id") or ""):
            return _json_error("Enter valid UPI ID or 10-digit phone number")
        if not db.update_member_upi(gid, mid, data.get("upi_id") or ""):
            return _json_error("Member not found", 404)
    return jsonify({"ok": True})


@app.post("/api/groups/<gid>/current-user")
def api_set_current_user(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    try:
        member_id = int(data.get("member_id"))
    except (TypeError, ValueError):
        return _json_error("Invalid member_id")
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM members WHERE id = ? AND group_id = ?",
            (member_id, gid),
        ).fetchone()
    if not row:
        return _json_error("Member not found", 404)
    session[_session_key(gid)] = member_id
    return jsonify({"ok": True, "current_user_id": member_id})


@app.post("/api/groups/<gid>/expenses")
def api_add_expense(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    payer_id = data.get("payer_id")
    try:
        payer_id = int(payer_id)
    except (TypeError, ValueError):
        return _json_error("Invalid payer")
    status = (data.get("status") or "").upper()
    if status not in ("PAID", "PENDING"):
        return _json_error("Status must be PAID or PENDING")
    parts = data.get("participant_ids")
    if parts is not None:
        try:
            parts = [int(x) for x in parts]
        except (TypeError, ValueError):
            return _json_error("Invalid participant_ids")
    ok, msg, eid = db.add_expense(
        gid,
        payer_id,
        data.get("description"),
        data.get("amount"),
        status,
        parts,
        (data.get("split_type") or "EQUAL").upper(),
    )
    if not ok:
        return _json_error(msg)
    return jsonify({"ok": True, "expense_id": eid})


@app.get("/api/groups/<gid>/upi/expense")
def api_upi_expense(gid):
    """Query params: payer_id, amount, description."""
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    try:
        payer_id = int(request.args.get("payer_id"))
        amount = float(request.args.get("amount", 0))
    except (TypeError, ValueError):
        return _json_error("Invalid payer or amount")
    desc = (request.args.get("description") or "").strip()
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT name, upi_id FROM members WHERE id = ? AND group_id = ?",
            (payer_id, gid),
        ).fetchone()
    if not row:
        return _json_error("Payer not found", 404)
    if not _is_upi_id(row["upi_id"] or ""):
        return _json_error("Payer UPI ID not set. Add valid UPI ID (example: name@upi)")
    link = upi_deep_link(row["upi_id"] or "", row["name"], amount, f"Splitzy payment: {desc}")
    return jsonify(
        {
            "ok": True,
            "upi_link": link,
            "payee_name": row["name"],
            "has_upi": bool(row["upi_id"]),
        }
    )


@app.post("/api/groups/<gid>/settlements")
def api_add_settlement(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    try:
        from_id = int(data.get("from_user"))
        to_id = int(data.get("to_user"))
        amount = float(data.get("amount"))
    except (TypeError, ValueError):
        return _json_error("Invalid settlement payload")
    current_user = _current_user_id(gid)
    if current_user != from_id:
        return _json_error("Only sender can initiate payment", 403)
    payment_type = (data.get("payment_type") or "UPI").upper()
    ok, msg, sid = db.create_transaction(
        gid,
        from_id,
        to_id,
        amount,
        payment_type,
        (data.get("transaction_ref") or "").strip() or None,
        (data.get("proof_image") or "").strip() or None,
    )
    if not ok:
        return _json_error(msg)
    return jsonify({"ok": True, "transaction_id": sid})


@app.post("/api/groups/<gid>/settlements/<int:sid>/claim")
def api_claim_settlement(gid, sid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    try:
        sender_id = int(data.get("sender_id"))
        receiver_id = int(data.get("receiver_id"))
        amount = float(data.get("amount"))
    except (TypeError, ValueError):
        return _json_error("Invalid payload")
    if _current_user_id(gid) != sender_id:
        return _json_error("Only sender can claim payment", 403)
    if not db.claim_transaction_paid(
        gid,
        sid,
        sender_id,
        (data.get("transaction_ref") or "").strip() or None,
        (data.get("proof_image") or "").strip() or None,
    ):
        return _json_error("Could not claim payment", 400)
    with db.get_db() as conn:
        sender = conn.execute(
            "SELECT name FROM members WHERE id = ? AND group_id = ?",
            (sender_id, gid),
        ).fetchone()
    sender_name = sender["name"] if sender else f"Member #{sender_id}"
    db.create_notification(
        gid,
        receiver_id,
        "PAYMENT_CLAIMED",
        f"{sender_name} marked INR {amount:.2f} as paid. Confirm receipt.",
    )
    return jsonify({"ok": True})


@app.post("/api/groups/<gid>/settlements/<int:sid>/confirm")
def api_confirm_settlement(gid, sid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    data = request.get_json(silent=True) or {}
    with db.get_db() as conn:
        tx = conn.execute(
            "SELECT receiver_id FROM transactions WHERE id = ? AND group_id = ?",
            (sid, gid),
        ).fetchone()
    if not tx:
        return _json_error("Transaction not found", 404)
    receiver_id = int(tx["receiver_id"])
    if _current_user_id(gid) != receiver_id:
        return _json_error("Only receiver can confirm/reject", 403)
    received = bool(data.get("received"))
    if not db.confirm_transaction(gid, sid, receiver_id, received):
        return _json_error("Could not update transaction", 400)
    return jsonify({"ok": True, "status": "SETTLED" if received else "PENDING"})


@app.post("/api/groups/<gid>/notifications/reminders")
def api_create_reminders(gid):
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    with db.get_db() as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.sender_id, t.amount, mr.name receiver_name
            FROM transactions t
            JOIN members mr ON mr.id = t.receiver_id
            WHERE t.group_id = ? AND t.status = 'PENDING'
            """,
            (gid,),
        ).fetchall()
    for r in rows:
        db.create_notification(
            gid,
            r["sender_id"],
            "PAYMENT_REMINDER",
            f"Reminder: pay {r['receiver_name']} INR {float(r['amount']):.2f} for tx #{r['id']}.",
        )
    return jsonify({"ok": True, "created": len(rows)})


@app.get("/api/groups/<gid>/upi/settlement")
def api_upi_settlement(gid):
    """Query: to_id, amount, note."""
    gid = gid.strip().upper()
    if not db.get_group(gid):
        return _json_error("Group not found", 404)
    try:
        to_id = int(request.args.get("to_id"))
        amount = float(request.args.get("amount", 0))
    except (TypeError, ValueError):
        return _json_error("Invalid parameters")
    note = (request.args.get("note") or "Splitzy payment").strip()
    group_name = (request.args.get("group_name") or "Splitzy").strip()
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT name, upi_id FROM members WHERE id = ? AND group_id = ?",
            (to_id, gid),
        ).fetchone()
    if not row:
        return _json_error("Payee not found", 404)
    if not _is_upi_id(row["upi_id"] or ""):
        return _json_error("Receiver UPI ID not set. Add valid UPI ID (example: name@upi)")
    link = upi_deep_link(
        row["upi_id"] or "",
        row["name"],
        amount,
        f"Splitzy payment for {group_name}",
    )
    return jsonify(
        {
            "ok": True,
            "upi_link": link,
            "payee_name": row["name"],
            "has_upi": bool(row["upi_id"]),
        }
    )


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# --- Razorpay placeholder (future verification) ---
@app.post("/api/payments/razorpay/webhook")
def razorpay_webhook_placeholder():
    """Reserved for verified payments; return 501 until configured."""
    return jsonify({"ok": False, "message": "Razorpay not configured"}), 501


db.init_db()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

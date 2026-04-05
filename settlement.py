"""Minimum-transaction settlement from net balances (greedy max-flow matching)."""


def minimize_settlements(net_balances: dict[int, float]) -> list[tuple[int, int, float]]:
    """
    net_balances: member_id -> net (positive = owed to them, negative = they owe).
    Returns list of (from_member_id, to_member_id, amount) meaning from pays to.
    """
    creditors: list[tuple[int, float]] = []
    debtors: list[tuple[int, float]] = []
    for mid, bal in net_balances.items():
        if bal > 0.005:
            creditors.append((mid, bal))
        elif bal < -0.005:
            debtors.append((mid, -bal))

    creditors.sort(key=lambda x: -x[1])
    debtors.sort(key=lambda x: -x[1])

    result: list[tuple[int, int, float]] = []
    i = j = 0
    while i < len(creditors) and j < len(debtors):
        cid, c_amt = creditors[i]
        did, d_amt = debtors[j]
        pay = min(c_amt, d_amt)
        if pay > 0.005:
            result.append((did, cid, round(pay, 2)))
        c_amt -= pay
        d_amt -= pay
        if c_amt < 0.005:
            i += 1
        else:
            creditors[i] = (cid, c_amt)
        if d_amt < 0.005:
            j += 1
        else:
            debtors[j] = (did, d_amt)

    return result

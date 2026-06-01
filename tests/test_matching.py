"""매칭 엔진 테스트: run_matching 패스들 + _find_split_combo 동치/성능."""
import random
import time
from itertools import combinations

import pandas as pd

import factories as F


def _parsed(app, scenario=F.basic_three_files):
    hansol, daily, patient = scenario()
    H = app.parse_hansol(hansol)
    D, _ = app.parse_daily(daily)
    P = app.parse_patient(patient)
    return H, D, P


def test_p1_approval_match(app):
    """결제메모 승인번호로 한솔↔차트 카드결제가 P1 매칭된다."""
    H, D, P = _parsed(app)
    match_df, matched_h, matched_dc = app.run_matching(H, D, P)
    assert not match_df.empty
    rules = match_df["매칭규칙"].tolist()
    assert any(r.startswith("P1_승인번호") for r in rules)
    # 카드 2건 매칭 + 현금영수증 1건
    assert (match_df["매칭규칙"] == "P1_승인번호").sum() == 2


def test_p2_unique_amount_match(app):
    """승인번호 연결이 없으면 유일 금액으로 P2 매칭된다."""
    hansol, daily, patient = F.basic_three_files()
    # 차트 결제메모에서 승인번호 제거 → P1 불가, 금액 유일 → P2
    patient.iloc[1, 5] = ""   # 100 메모 제거
    patient.iloc[2, 5] = ""   # 200 메모 제거
    H = app.parse_hansol(hansol)
    D, _ = app.parse_daily(daily)
    P = app.parse_patient(patient)
    match_df, _, _ = app.run_matching(H, D, P)
    assert (match_df["매칭규칙"] == "P2_유일금액").sum() == 2


def test_split_payment_p3(app):
    """한 차트 카드금액 = 한솔 2건 합(시간 근접) → P3 분할매칭."""
    hansol = F.hansol_raw(
        금액=[30000, 20000],
        승인번호=["111111", "222222"],
        거래시간=["100000", "100200"],          # 2분 차이
        거래상태=["정상승인", "정상승인"],
        구분=["카드", "카드"],
        매입사=["삼성카드", "삼성카드"],
        카드번호=["1111222233334444", "1111222233334444"],
    )
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드"],
        [1, "500", "분할환자", 50000],            # 30000 + 20000
    )
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["500", "분할환자", "카드-삼성", 50000, ""],
    )
    H = app.parse_hansol(hansol)
    D, _ = app.parse_daily(daily)
    P = app.parse_patient(patient)
    match_df, matched_h, _ = app.run_matching(H, D, P)
    assert (match_df["매칭규칙"] == "P3_분할2건").any()
    assert len(matched_h) == 2                      # 한솔 2건 모두 매칭


# ── _find_split_combo: 전수탐색(combinations)과 완전 동치임을 보장 (회귀 방지) ──
def _brute(items, target, rs, window):
    for r in rs:
        if len(items) < r:
            continue
        for combo in combinations(range(len(items)), r):
            its = [items[k] for k in combo]
            if sum(it[1] for it in its) == target:
                times = [it[2] for it in its]
                if (max(times) - min(times)) <= window:
                    return its
    return None


def test_find_split_combo_equivalence_fuzz(app):
    mism = 0
    for trial in range(4000):
        random.seed(trial)
        n = random.randint(2, 11)
        items = [[100 + i, random.choice([1, 2, 3, 5, 7, 11, 22]) * 1000,
                  random.randint(0, 30)] for i in range(n)]
        target = (random.choice([it[1] for it in items])
                  + random.choice([0, 1000, 2000, 3000, 5000, 11000]))
        window = random.choice([5, 10, 15])
        a = _brute(items, target, [2, 3], window)
        b = app._find_split_combo(items, target, [2, 3], window)
        sa = tuple(sorted(x[0] for x in a)) if a else None
        sb = tuple(sorted(x[0] for x in b)) if b else None
        assert sa == sb, f"trial={trial} target={target} win={window} items={items}"


def test_find_split_combo_large_input_is_fast(app):
    """미매칭 후보가 많아도(최악) 빠르게 종료해야 한다 (이전 O(n^3) → 수십초 회귀 방지)."""
    random.seed(0)
    items = [[i, random.randint(1, 60) * 1000, i % 600] for i in range(350)]
    t0 = time.time()
    for _ in range(60):                              # 60개 타깃 전부 미매칭 강제
        app._find_split_combo(items, 7_000_007, [2, 3], 10)
    assert time.time() - t0 < 5.0                    # 넉넉한 상한(실측 ~1초 이하)

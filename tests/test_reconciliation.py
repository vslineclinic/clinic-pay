"""합계/채널대사/의심후보 테스트 (★메인 산출물의 정확도)."""
import pandas as pd

import factories as F


def _pipeline(app, scenario):
    hansol, daily, patient = scenario()
    H = app.parse_hansol(hansol)
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(H, D, DR, P)
    channel = app.compute_channel_recon(totals)
    return H, D, DR, P, totals, channel


def test_totals_basic(app):
    _, _, _, _, totals, _ = _pipeline(app, F.basic_three_files)
    assert totals["h_card"] == 80000      # 50000 + 30000
    assert totals["h_cash"] == 20000
    assert totals["d_card"] == 80000
    assert totals["p_card"] == 80000
    assert totals["p_cashxfer"] == 20000
    assert totals["_has_hansol"] is True


def test_channel_recon_all_match(app):
    _, _, _, _, _, channel = _pipeline(app, F.basic_three_files)
    card = channel[channel["채널"] == "카드"].iloc[0]
    assert card["한솔-차트"] == 0
    assert card["한솔-일마"] == 0
    assert card["일마-차트"] == 0


def test_totals_net_of_refund(app):
    """환불/취소가 채널 합계에서 net 처리되는지 (일마 환불섹션 + 차트 취소행)."""
    hansol = F.hansol_raw(
        금액=[50000, 30000], 승인번호=["100111", "200222"],
        거래시간=["143025", "150010"], 거래상태=["정상승인", "정상승인"],
        구분=["카드", "카드"], 매입사=["삼성카드", "현대카드"],
    )
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        [2, "200", "이영희", 30000, 0, 0],
        ["환불/취소 내역", "", "", "", "", ""],
        ["구분", "차트번호", "성명", "카드", "현금", "이체"],
        ["환불", "200", "이영희", 10000, 0, 0],
    )
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드-삼성", 50000, ""],
        ["200", "이영희", "카드-현대", 30000, ""],
        ["200", "이영희", "카드취소", 10000, ""],
    )
    H = app.parse_hansol(hansol)
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(H, D, DR, P)
    assert totals["d_card"] == 70000        # 80000 - 10000 환불
    assert totals["p_card"] == 70000        # 80000 - 10000 취소


def test_two_file_mode_no_hansol(app):
    """한솔 없이 일마↔차트 2파일만으로 합계 산출."""
    _, daily, patient = F.basic_three_files()
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(pd.DataFrame(), D, DR, P)
    assert totals["_has_hansol"] is False
    assert totals["h_card"] is None
    channel = app.compute_channel_recon(totals)
    card = channel[channel["채널"] == "카드"].iloc[0]
    assert card["한솔"] is None
    assert card["일마-차트"] == 0


def test_star2_suspect_on_mismatch(app):
    """차트만 5,000원 많을 때 승인번호로 ★★ 동일환자 확정 후보가 잡힌다."""
    hansol, daily, patient = F.mismatch_three_files()
    H = app.parse_hansol(hansol)
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(H, D, DR, P)
    suspects = app.find_channel_suspects("카드", H, D, P, totals=totals, top_n=15)
    star2 = [s for s in suspects if "★★" in str(s.get("출처", ""))]
    assert star2, "승인번호 cross-match ★★ 후보가 있어야 함"
    top = star2[0]
    assert top["금액"] == -5000                       # 한솔 30000 - 차트 35000
    assert "200" in str(top.get("환자", "")) or "이영희" in str(top.get("환자", ""))


# ── 유형B 병합·환불 net·존재위치 태그 (차트 기준 원칙) ──────────────


def test_typeB_pair_merged_out_of_suspects(app):
    """일일마감 차트번호만 오타(이름·금액 동일)면 차이 목록에 잡히지 않는다.

    차트마감(EMR)은 차트번호 오입력이 불가능하므로 유형B는 [데이터검증]의
    번호 정정 안내로만 보고하고, 채널/환자 차이 금액에는 포함하지 않는다."""
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "1000", "김철수", 50000, 0, 0],   # 100 → 1000 수기오타
        [2, "200", "이영희", 30000, 0, 0],
    )
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드-삼성", 50000, ""],
        ["200", "이영희", "카드-현대", 30000, ""],
    )
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    pp, dp = app._chart_method_pivots(P, D, DR)
    assert "1000" not in dp and dp["100"]["카드"] == 50000  # 차트마감 번호로 귀속
    totals = app.compute_totals(pd.DataFrame(), D, DR, P)
    suspects = app.find_channel_suspects(
        "카드", pd.DataFrame(), D, P, totals=totals, top_n=15, daily_refund=DR)
    assert not [s for s in suspects if "차트↔일마" in s["출처"]]
    # 유형B 안내 자체는 데이터검증에 남아야 함
    verif = app.build_verification(P, D, DR)
    assert len(verif["유형B_차트번호오타"]) == 1


def test_pivot_nets_daily_refund_no_false_suspect(app):
    """차트 결제+취소(net 0) ↔ 일마 결제+환불행(net 0)이 허위 차이로 잡히지 않는다."""
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        ["환불/취소 내역", "", "", "", "", ""],
        ["구분", "차트번호", "성명", "카드", "현금", "이체"],
        ["환불", "100", "김철수", 50000, 0, 0],
    )
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드-삼성", 50000, ""],
        ["100", "김철수", "카드취소", 50000, ""],
    )
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(pd.DataFrame(), D, DR, P)
    suspects = app.find_channel_suspects(
        "카드", pd.DataFrame(), D, P, totals=totals, top_n=15, daily_refund=DR)
    assert not [s for s in suspects if "차트↔일마" in s["출처"]]


def test_suspect_presence_tag_daily_only(app):
    """일일마감에만 있는 수납은 '[일일마감에만 존재]'로 위치를 명시한다."""
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        [2, "900", "박없음", 70000, 0, 0],   # 차트에 없는 환자 (이름·금액 모두 상이)
    )
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드-삼성", 50000, ""],
    )
    D, DR = app.parse_daily(daily)
    P = app.parse_patient(patient)
    totals = app.compute_totals(pd.DataFrame(), D, DR, P)
    suspects = app.find_channel_suspects(
        "카드", pd.DataFrame(), D, P, totals=totals, top_n=15, daily_refund=DR)
    rows = [s for s in suspects if "차트↔일마" in s["출처"]]
    assert len(rows) == 1
    assert "[일일마감에만 존재]" in rows[0]["단서"]
    assert "차트(세무 기준)" in rows[0]["조치"]

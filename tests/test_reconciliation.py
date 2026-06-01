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

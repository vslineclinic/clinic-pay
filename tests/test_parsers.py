"""파서 테스트: parse_hansol / parse_daily / parse_patient."""
import pandas as pd

import factories as F


def test_parse_hansol_basic(app):
    raw = F.hansol_raw(
        금액=[50000, 30000, 10000],
        승인번호=["100111", "200222", ""],          # 빈 승인번호 → 미승인으로 제외
        거래시간=["143025", "150010", "160000"],
        거래상태=["정상승인", "취소", "정상승인"],
        구분=["카드", "카드", "카드"],
        매입사=["삼성카드", "현대카드", "국민카드"],
    )
    h = app.parse_hansol(raw)
    assert len(h) == 2                                  # 빈 승인번호 행 제외됨
    assert set(h["tx_status"]) == {"정상", "취소"}
    assert h.iloc[0]["시간_분"] == 14 * 60 + 30         # 시간 파싱 검증
    assert (~h["is_현금"]).all()                        # 모두 카드


def test_parse_hansol_cash_receipt_split(app):
    raw = F.hansol_raw(
        금액=[20000],
        승인번호=["300333"],
        거래시간=["151500"],
        거래상태=["정상승인"],
        구분=["현금"],
        매입사=["현금영수증"],
    )
    h = app.parse_hansol(raw)
    assert bool(h.iloc[0]["is_현금"]) is True           # 현금영수증 → is_현금


def test_parse_daily_filters_total_row(app):
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        [2, "200", "이영희", 30000, 0, 0],
        ["", "합계", "", 80000, 0, 0],                  # 합계행 → 제외돼야 함
    )
    d, refund = app.parse_daily(daily)
    assert len(d) == 2
    assert int(d["카드"].sum()) == 80000
    assert "합계" not in set(d["성명"])


def test_parse_daily_refund_section(app):
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        [2, "200", "이영희", 30000, 0, 0],
        ["환불/취소 내역", "", "", "", "", ""],
        ["구분", "차트번호", "성명", "카드", "현금", "이체"],
        ["환불", "200", "이영희", 10000, 0, 0],
    )
    d, refund = app.parse_daily(daily)
    assert len(d) == 2                                  # 메인 데이터는 환불 섹션 분리
    assert not refund.empty
    assert int(refund["카드"].sum()) == 10000


def test_parse_daily_recovers_unlabeled_chart_column(app):
    # 실제 지점 시트(인천/잠실/엔디어트/강남)는 '순서|구분|차트번호|성명' 구조인데
    # 차트번호 열의 머리글 칸만 비어 있다 → '구분' 오른쪽 무라벨 숫자열을 차트번호로 복구.
    daily = F.table_raw(
        ["내원순서", "구분", "", "성명", "카드"],          # 3번째(차트번호) 머리글 비어 있음
        [1, "신환", "66909", "진시인", 50000],
        [2, "구환", "61302", "김소정", 30000],
        [3, "구환", "65251", "류혜정", 20000],
    )
    d, _ = app.parse_daily(daily)
    assert "차트번호" in d.columns
    assert d["차트번호"].tolist() == ["66909", "61302", "65251"]


def test_parse_daily_chart_recovery_ignores_decoy_column(app):
    # '이름/차트번호 중복여부'(머리글 있는 decoy)를 차트번호로 오인하면 안 된다.
    daily = F.table_raw(
        ["내원순서", "구분", "", "성명", "카드", "이름/차트번호 중복여부"],
        [1, "신환", "66909", "진시인", 50000, "정상"],
        [2, "구환", "61302", "김소정", 30000, "중복"],
        [3, "구환", "65251", "류혜정", 20000, "정상"],
    )
    d, _ = app.parse_daily(daily)
    assert d["차트번호"].tolist() == ["66909", "61302", "65251"]   # decoy('정상'/'중복') 아님


def test_parse_daily_chart_recovery_skips_nonnumeric_unlabeled(app):
    # '구분' 오른쪽 무라벨 열이 숫자가 아니면(텍스트) 차트번호로 복구하지 않는다.
    daily = F.table_raw(
        ["내원순서", "구분", "", "성명", "카드"],
        [1, "신환", "전화상담", "진시인", 50000],
        [2, "구환", "워크인", "김소정", 30000],
        [3, "구환", "재진", "류혜정", 20000],
    )
    d, _ = app.parse_daily(daily)
    s = d["차트번호"].astype(str).str.strip()
    assert ((s == "") | (s.str.lower() == "nan")).all()   # 복구 안 함 → 빈 값


def test_parse_patient_payment_classification(app):
    _, _, patient = F.basic_three_files()
    p = app.parse_patient(patient)
    assert list(p["분류"]) == ["카드", "카드", "현금"]
    assert p.iloc[0]["카드사"] == "삼성"
    assert p.iloc[0]["승인번호목록"] == ["100111"]      # 결제메모에서 승인번호 추출


def test_parse_patient_cancellation_makes_amount_negative(app):
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드-삼성", 50000, ""],
        ["200", "이영희", "카드취소", 10000, ""],        # 취소행
    )
    p = app.parse_patient(patient)
    assert bool(p.iloc[1]["is_취소"]) is True
    assert p.iloc[1]["금액"] == -10000                  # 취소는 음수


def test_parse_patient_missing_chart_column_no_crash(app):
    """차트번호 컬럼이 없는 export에서도 KeyError 없이 동작 (회귀 방지)."""
    patient = F.table_raw(
        ["이름", "결제수단", "비급여(과세총금액)"],
        ["김철수", "카드-삼성", 50000],
        ["이영희", "현금", 20000],
    )
    p = app.parse_patient(patient)                      # 예외 없어야 함
    assert list(p["차트번호"]) == ["", ""]
    assert list(p["분류"]) == ["카드", "현금"]


def test_parse_patient_missing_name_column_no_crash(app):
    patient = F.table_raw(
        ["차트번호", "결제수단", "비급여(과세총금액)"],
        ["100", "카드-삼성", 50000],
    )
    p = app.parse_patient(patient)
    assert list(p["이름"]) == [""]
    assert list(p["차트번호"]) == ["100"]

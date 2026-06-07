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


def test_parse_daily_recovers_pay_cols_from_summary_block(app):
    # 인천형: 결제수단 열 머리글이 모두 비어 있고, 하단 '세로 요약블록'(채널명+합계)에만
    # 라벨이 있다. 무라벨 금액열의 환자합계를 요약블록 합계와 대조해 카드/현금을 복원한다.
    daily = F.table_raw(
        ["내원순서", "구분", "", "성명", "", ""],      # 차트번호·카드(4)·현금(5) 머리글 공란
        [1, "구환", "100", "김철수", 50000, 0],        # 카드 50000
        [2, "신환", "200", "이영희", 30000, 0],        # 카드 30000
        [3, "구환", "300", "박민수", 0, 20000],        # 현금 20000
        ["", "", "", "", "", ""],
        ["", "카드", "", "", "80000", ""],             # 요약블록: 카드 80,000
        ["", "현금", "", "", "20000", ""],             # 요약블록: 현금 20,000
        ["", "이체", "", "", "0", ""],                 # 요약블록: 이체 0(매칭 불가→스킵)
    )
    d, _ = app.parse_daily(daily)
    assert int(d["카드"].sum()) == 80000
    assert int(d["현금"].sum()) == 20000
    assert int(d["총액"].sum()) == 100000


def test_parse_daily_summary_recovery_handles_aliases(app):
    # 엔디어트형: 요약블록이 약어/별칭('결제단말기'=카드, '강.언'=강남언니)을 쓰고, 금액열은
    # 머리글이 비어 있다. 별칭을 표준 채널로 정규화해 합계 대조로 복원해야 한다.
    daily = F.table_raw(
        ["내원순서", "구분", "", "성명", "", ""],
        [1, "구환", "100", "김철수", 50000, 0],        # 카드 50000
        [2, "신환", "200", "이영희", 30000, 0],        # 카드 30000
        [3, "구환", "300", "박민수", 0, 5000],         # 강남언니 5000
        ["", "", "", "", "", ""],
        ["", "결제단말기", "", "", "80000", ""],       # 카드 별칭
        ["", "강.언", "", "", "5000", ""],             # 강남언니 약어
        ["", "현금", "", "", "0", ""],
    )
    d, _ = app.parse_daily(daily)
    assert int(d["카드"].sum()) == 80000
    assert int(d["강남언니"].sum()) == 5000


def test_channel_of_normalizes_aliases(app):
    assert app._channel_of("결제단말기") == "카드"
    assert app._channel_of("나만의 닥터") == "나만의닥터"     # 공백 무시
    assert app._channel_of("강.언") == "강남언니"            # 점 무시
    assert app._channel_of("여신") == "여신티켓"
    assert app._channel_of("기타-지역화폐") == "기타지역화폐"  # 하이픈 무시
    assert app._channel_of("성명") is None
    assert app._channel_of("현금시재액") is None             # 부분일치 오인 금지


def test_summary_recovery_inert_on_standard_form(app):
    # 표준 양식(요약블록 없음, 머리글에만 채널)은 재라벨링이 작동하지 않아야 한다.
    standard = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "현금", "이체"],
        [1, "100", "김철수", 50000, 0, 0],
        [2, "200", "이영희", 0, 30000, 0],
    )
    assert app._summary_channel_totals(standard, 0) == {}       # 요약블록 미검출
    d, _ = app.parse_daily(standard)
    assert int(d["카드"].sum()) == 50000
    assert int(d["현금"].sum()) == 30000


def test_parse_patient_payment_classification(app):
    _, _, patient = F.basic_three_files()
    p = app.parse_patient(patient)
    assert list(p["분류"]) == ["카드", "카드", "현금"]
    assert p.iloc[0]["카드사"] == "삼성"
    assert p.iloc[0]["승인번호목록"] == ["100111"]      # 결제메모에서 승인번호 추출


# ── QR·모바일 간편결제(알리페이/위챗페이/카카오페이) = 플랫폼 집계 ──────────

def test_channel_of_normalizes_qr_platform_aliases(app):
    assert app._channel_of("알리페이") == "알리페이"
    assert app._channel_of("위챗 페이") == "위챗페이"      # 공백 무시
    assert app._channel_of("위쳇페이") == "위챗페이"        # 흔한 오기
    assert app._channel_of("카카오페이") == "카카오페이"


def test_parse_daily_counts_qr_platform_channels(app):
    # 압구정형: 알리페이/위챗페이/카카오페이도 플랫폼합·총액에 집계돼야 한다.
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "여신티켓", "알리페이", "위챗페이", "카카오페이"],
        [1, "100", "김철수", 10000, 0, 50000, 0, 0],
        [2, "200", "이영희", 0, 3000, 0, 20000, 7000],
    )
    d, _ = app.parse_daily(daily)
    assert int(d["알리페이"].sum()) == 50000
    assert int(d["위챗페이"].sum()) == 20000
    assert int(d["카카오페이"].sum()) == 7000
    # 플랫폼합 = 여신티켓3000 + 알리50000 + 위챗20000 + 카카오7000 = 80000
    assert int(d["플랫폼합"].sum()) == 80000
    # 총액 = 카드10000 + 플랫폼합80000 = 90000
    assert int(d["총액"].sum()) == 90000


def test_parse_patient_classifies_qr_platforms(app):
    # 차트마감에서 알리페이/위챗페이/카카오페이는 '플랫폼'으로 분류돼야(일일마감과 대칭).
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "본부금", "결제메모"],
        ["100", "김철수", "알리페이", 50000, 0, ""],
        ["200", "이영희", "위챗페이", 20000, 0, ""],
        ["300", "박민수", "카카오페이", 7000, 0, ""],
        ["400", "최지우", "카드-삼성카드", 10000, 0, ""],
    )
    p = app.parse_patient(patient)
    cls = dict(zip(p["차트번호"], p["분류"]))
    assert cls["100"] == "플랫폼"
    assert cls["200"] == "플랫폼"
    assert cls["300"] == "플랫폼"
    assert cls["400"] == "카드"     # 카드 결제는 영향 없음


def test_parse_patient_kanpyeon_cash_receipt_is_platform_not_cash(app):
    # 베가스 차트마감의 '간편결제(현금영수증)'은 QR 간편결제(큐릭)이므로 '현금영수증'
    # 글자가 있어도 현금이 아니라 플랫폼으로 분류돼야 한다(일일마감 '간편결제(큐릭)'과 대칭).
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "본부금", "결제메모"],
        ["100", "리샤", "간편결제(현금영수증)", 4092000, 0, ""],
        ["200", "이예호", "현금(현금영수증)", 550000, 0, ""],   # 진짜 현금은 현금 유지
    )
    p = app.parse_patient(patient)
    cls = dict(zip(p["차트번호"], p["분류"]))
    assert cls["100"] == "플랫폼"     # 간편결제 → 플랫폼
    assert cls["200"] == "현금"       # 일반 현금영수증 → 현금


def test_parse_daily_counts_kanpyeon_babitalk_doctornow(app):
    # 강남형: 간편결제(큐릭)/바비톡/닥터나우도 플랫폼합·총액에 집계돼야 한다.
    daily = F.table_raw(
        ["내원순서", "차트번호", "성명", "카드", "간편결제(큐릭)", "바비톡", "닥터나우"],
        [1, "100", "김철수", 10000, 50000, 0, 0],
        [2, "200", "이영희", 0, 0, 30000, 7000],
    )
    d, _ = app.parse_daily(daily)
    assert int(d["간편결제"].sum()) == 50000
    assert int(d["바비톡"].sum()) == 30000
    assert int(d["닥터나우"].sum()) == 7000
    # 플랫폼합 = 50000 + 30000 + 7000 = 87000, 총액 = 카드10000 + 87000 = 97000
    assert int(d["플랫폼합"].sum()) == 87000
    assert int(d["총액"].sum()) == 97000


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

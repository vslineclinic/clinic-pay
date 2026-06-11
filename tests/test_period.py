"""기간(다일) 대사 테스트: norm_date / 날짜 컬럼 / compute_period_recon /
find_period_day_detail / filter_to_single_date + 실데이터에서 발견된 분류 버그 회귀."""
from datetime import date

import pandas as pd

import factories as F


# ── norm_date ────────────────────────────────────────────────

def test_norm_date_variants(app):
    assert app.norm_date("2026-06-10(수)") == "2026-06-10"   # 차트마감 수납일
    assert app.norm_date("260610") == "2026-06-10"           # 한솔 거래일 YYMMDD
    assert app.norm_date(260610) == "2026-06-10"             # 엑셀 숫자 저장
    assert app.norm_date("260610.0") == "2026-06-10"
    assert app.norm_date("2026.06.10") == "2026-06-10"
    assert app.norm_date("26.6.2") == "2026-06-02"
    assert app.norm_date(pd.Timestamp("2026-06-10 14:30")) == "2026-06-10"
    assert app.norm_date(None) == ""
    assert app.norm_date("") == ""
    assert app.norm_date("합계") == ""
    assert app.norm_date("261490") == ""                     # 14월 → 무효


def test_parse_hansol_date_column(app):
    raw = F.hansol_raw(
        금액=[50000, 30000],
        승인번호=["100111", "200222"],
        거래일=["260511", "260512"],
        거래시간=["143025", "150010"],
        거래상태=["정상승인", "정상승인"],
        구분=["카드", "카드"],
        매입사=["삼성카드", "현대카드"],
    )
    h = app.parse_hansol(raw)
    assert list(h["날짜"]) == ["2026-05-11", "2026-05-12"]


def test_parse_patient_date_uses_sunabil_not_jinryoil(app):
    """날짜는 반드시 수납일 기준 (진료일은 외상·선결제로 다를 수 있음)."""
    patient = F.table_raw(
        ["수납일", "진료일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-06-10(수)", "2026-06-08(월)", "100", "김철수", "카드(삼성)", 50000, ""],
    )
    p = app.parse_patient(patient)
    assert p.iloc[0]["날짜"] == "2026-06-10"


# ── 실데이터에서 발견된 분류 버그 회귀 ───────────────────────

def test_refund_tongjang_classified_as_transfer(app):
    """'결제취소-통장'/'환불-통장'은 이체로 분류·차감돼야 함(인천 5/11 -599,000 사례)."""
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김소운", "결제취소-통장", 599000, ""],
        ["200", "신이재", "환불-통장", 101000, ""],
        ["300", "박민수", "통장입금(현금영수증)", 50000, ""],
    )
    p = app.parse_patient(patient)
    assert list(p["분류"]) == ["이체", "이체", "이체"]
    assert bool(p.iloc[0]["is_취소"]) and bool(p.iloc[1]["is_취소"])
    assert int(p.iloc[0]["금액"]) == -599000               # 취소 → 음수


def test_refund_card_keeps_card_despite_platform_memo(app):
    """'환불-카드' + 메모에 플랫폼 단어(결제수단 변경 경위)가 있어도 카드 유지
    (엔디어트 5/26 -198,000 사례: 카드 취소 후 여신앱 재결제)."""
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "이경연", "환불-카드", 198000, "33754957 /// 카드 취소후 여신앱으로 결제 변경"],
        ["100", "이경연", "기타(기타)", 198000, "여신앱// 카드결제 => 여신앱 변경"],
    )
    p = app.parse_patient(patient)
    assert p.iloc[0]["분류"] == "카드"                     # 환불-카드는 카드 유지
    assert int(p.iloc[0]["금액"]) == -198000
    assert p.iloc[1]["분류"] == "플랫폼"                   # 기타+메모 여신앱 → 플랫폼


def test_refund_gita_classified_as_platform(app):
    """'환불-기타'는 양(+)의 '기타(기타)'(플랫폼)와 대칭으로 플랫폼에서 차감."""
    patient = F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김서연", "환불-기타", 198000, ""],
    )
    p = app.parse_patient(patient)
    assert p.iloc[0]["분류"] == "플랫폼"
    assert int(p.iloc[0]["금액"]) == -198000


# ── 기간 대사 ────────────────────────────────────────────────

def _two_day_files(app):
    """5/11(일치) · 5/12(차트 카드 +9,000 오입력) 2일 시나리오."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[50000, 30000, 20000, 40000],
        승인번호=["100111", "200222", "300333", "400444"],
        거래일=["260511", "260511", "260512", "260512"],
        거래시간=["100000", "110000", "100000", "110000"],
        거래상태=["정상승인", "정상승인", "정상승인", "정상승인"],
        구분=["카드", "카드", "카드", "카드"],
        매입사=["삼성카드", "현대카드", "삼성카드", "국민카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-11(월)", "100", "김철수", "카드(삼성)", 50000, "100111"],
        ["2026-05-11(월)", "200", "이영희", "카드(현대)", 30000, "200222"],
        ["2026-05-12(화)", "300", "박민수", "카드(삼성)", 20000, "300333"],
        ["2026-05-12(화)", "400", "최지우", "카드(국민)", 49000, ""],  # 40,000 오입력
    ))
    return hansol, patient


def test_compute_period_recon(app):
    hansol, patient = _two_day_files(app)
    t = app.compute_period_recon(hansol, patient)
    assert list(t["날짜"]) == ["2026-05-11", "2026-05-12"]
    d1 = t[t["날짜"] == "2026-05-11"].iloc[0]
    assert d1["한솔카드"] == 80000 and d1["차트카드"] == 80000 and d1["카드차이"] == 0
    d2 = t[t["날짜"] == "2026-05-12"].iloc[0]
    assert d2["한솔카드"] == 60000 and d2["차트카드"] == 69000 and d2["카드차이"] == -9000


def test_compute_period_recon_cancel_nets(app):
    """한솔 취소는 차감, 차트 환불도 차감 → 동일 net."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[50000, 50000],
        승인번호=["100111", "100111"],
        거래일=["260511", "260511"],
        거래시간=["100000", "120000"],
        거래상태=["정상승인", "취소승인"],
        구분=["카드", "카드"],
        매입사=["삼성카드", "삼성카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-11(월)", "100", "김철수", "카드(삼성)", 50000, "100111"],
        ["2026-05-11(월)", "100", "김철수", "환불-카드", 50000, "100111"],
    ))
    t = app.compute_period_recon(hansol, patient)
    d = t.iloc[0]
    assert d["한솔카드"] == 0 and d["차트카드"] == 0 and d["카드차이"] == 0


def test_find_period_day_detail_isolates_cause(app):
    hansol, patient = _two_day_files(app)
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-05-12")
    # 300/20,000은 승인번호로 설명 → 남는 건 한솔 40,000 vs 차트 49,000 한 쌍
    assert len(un_h) == 1 and int(un_h.iloc[0]["금액"]) == 40000
    assert len(un_p) == 1 and int(un_p.iloc[0]["금액"]) == 49000

    # 일치하는 날은 미설명 0건
    un_h0, un_p0 = app.find_period_day_detail(hansol, patient, "2026-05-11")
    assert un_h0.empty and un_p0.empty


def test_find_period_day_detail_self_cancel_pair(app):
    """당일 결제+당일 취소(동일 승인번호) 쌍은 차트 흔적 없어도 '설명됨' 처리."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[363000, 363000, 50000],
        승인번호=["49121125", "49121125", "100111"],
        거래일=["260526", "260526", "260526"],
        거래시간=["162302", "171007", "100000"],
        거래상태=["정상승인", "취소승인", "정상승인"],
        구분=["카드", "카드", "카드"],
        매입사=["삼성카드", "삼성카드", "삼성카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-26(화)", "100", "김철수", "카드(삼성)", 50000, "100111"],
    ))
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-05-26")
    assert un_h.empty and un_p.empty


# ── 오류 건 자동 특정 (미설명 잔여 건 오타쌍 페어링) ─────────

def test_amount_typo_kind_patterns(app):
    assert "자릿수" in app._amount_typo_kind(50000, 500000)     # 0 추가/누락
    assert "한 자리" in app._amount_typo_kind(40000, 49000)     # 한 자리 오타
    assert "뒤바뀜" in app._amount_typo_kind(120000, 210000)    # 인접 자리 전치
    assert app._amount_typo_kind(40000, 40000) == ""            # 동일 금액
    assert app._amount_typo_kind(40000, 73000) == ""            # 무관한 금액(편집거리 2)
    assert app._amount_typo_kind(0, 50000) == ""


def test_pair_period_typo_suspects_amount_typo(app):
    """5/12 미설명 쌍(한솔 40,000 ↔ 차트 49,000)을 같은 건의 금액 오타로 특정."""
    hansol, patient = _two_day_files(app)
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-05-12")
    pairs = app.pair_period_typo_suspects(un_h, un_p)
    assert len(pairs) == 1
    r = pairs.iloc[0]
    assert r["차트번호"] == "400" and r["환자"] == "최지우"
    assert r["차트금액"] == 49000 and r["한솔금액"] == 40000
    assert r["차이(한솔-차트)"] == -9000
    assert "오타" in r["추정원인"]


def test_pair_period_typo_suspects_direction_mismatch(app):
    """금액 동일·방향 상이(한솔 정상 ↔ 차트 환불)는 환불방향 불일치로 특정."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[80000],
        승인번호=["700777"],
        거래일=["260511"],
        거래시간=["100000"],
        거래상태=["정상승인"],
        구분=["카드"],
        매입사=["삼성카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-11(월)", "500", "정환불", "결제취소-카드", 80000, ""],
    ))
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-05-11")
    pairs = app.pair_period_typo_suspects(un_h, un_p)
    assert len(pairs) == 1
    r = pairs.iloc[0]
    assert r["차트금액"] == -80000 and r["한솔금액"] == 80000
    assert "환불방향" in r["추정원인"]


def test_pair_period_typo_suspects_no_false_pair(app):
    """무관한 금액끼리는 페어링하지 않는다(허위 특정 방지)."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[40000],
        승인번호=["100111"],
        거래일=["260511"],
        거래시간=["100000"],
        거래상태=["정상승인"],
        구분=["카드"],
        매입사=["삼성카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-11(월)", "100", "김철수", "카드(삼성)", 73000, ""],
    ))
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-05-11")
    pairs = app.pair_period_typo_suspects(un_h, un_p)
    assert pairs.empty


# ── 기간 분석 × 구글시트 일일마감 비교 ───────────────────────

def _fake_gsheet_loader(tabs):
    """tabs: {date: raw DataFrame}. 등록되지 않은 날짜는 시트 탭 없음(LookupError)."""
    def loader(url_or_id, picked_date, timeout=20, cache=None):
        if picked_date in tabs:
            return tabs[picked_date], picked_date.strftime("%y.%m.%d")
        raise LookupError("탭 없음")
    return loader


def test_augment_period_with_gsheet_basic(app):
    """시트가 있는 날은 일마 합계·차이 컬럼이 붙고, 없는 날은 빼고(사유 기록) 검증."""
    hansol, patient = _two_day_files(app)
    table = app.compute_period_recon(hansol, patient)
    tabs = {
        date(2026, 5, 11): F.table_raw(
            ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
            [1, "100", "김철수", "", 50000, 0, 0],
            [2, "200", "이영희", "", 30000, 0, 0],
        ),
    }
    t2, skipped, day_data = app.augment_period_with_gsheet(
        table, patient, "sheet-id", loader=_fake_gsheet_loader(tabs))
    d1 = t2[t2["날짜"] == "2026-05-11"].iloc[0]
    assert d1["일마카드"] == 80000
    assert d1["일마-차트카드차이"] == 0
    assert d1["일마비고"] == ""
    # 5/12는 시트 탭 없음 → 비교 제외 + 사유 기록 (나머지 날만 분석)
    assert skipped == {"2026-05-12": "시트없음"}
    d2 = t2[t2["날짜"] == "2026-05-12"].iloc[0]
    assert pd.isna(d2["일마카드"]) and d2["일마비고"] == "시트없음"
    assert "2026-05-11" in day_data and "2026-05-12" not in day_data


def test_augment_period_with_gsheet_detects_daily_error(app):
    """일마 금액이 차트와 다르면 일마-차트카드차이로 잡힌다."""
    hansol, patient = _two_day_files(app)
    table = app.compute_period_recon(hansol, patient)
    tabs = {
        date(2026, 5, 11): F.table_raw(
            ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
            [1, "100", "김철수", "", 50000, 0, 0],
            [2, "200", "이영희", "", 40000, 0, 0],   # 30,000 → 40,000 오입력
        ),
    }
    t2, skipped, _ = app.augment_period_with_gsheet(
        table, patient, "sheet-id", loader=_fake_gsheet_loader(tabs))
    d1 = t2[t2["날짜"] == "2026-05-11"].iloc[0]
    assert d1["일마카드"] == 90000
    assert d1["일마-차트카드차이"] == 10000


def test_augment_period_with_gsheet_wrong_sheet_excluded(app):
    """차트번호가 안 겹치는 시트(다른 지점/날짜 잘못 입력)는 대조불일치로 제외."""
    hansol, patient = _two_day_files(app)
    table = app.compute_period_recon(hansol, patient)
    tabs = {
        date(2026, 5, 11): F.table_raw(
            ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
            [1, "900", "박타인", "", 70000, 0, 0],
            [2, "901", "최타인", "", 10000, 0, 0],
        ),
    }
    t2, skipped, day_data = app.augment_period_with_gsheet(
        table, patient, "sheet-id", loader=_fake_gsheet_loader(tabs))
    assert skipped["2026-05-11"].startswith("대조불일치")
    assert not day_data
    d1 = t2[t2["날짜"] == "2026-05-11"].iloc[0]
    assert pd.isna(d1["일마카드"])


def test_augment_period_with_gsheet_nets_refund(app):
    """일마 환불 행은 차감(net)해서 차트와 비교한다."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[50000], 승인번호=["100111"], 거래일=["260511"],
        거래시간=["100000"], 거래상태=["정상승인"], 구분=["카드"], 매입사=["삼성카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-05-11(월)", "100", "김철수", "카드(삼성)", 50000, "100111"],
        ["2026-05-11(월)", "200", "이영희", "카드(현대)", 30000, ""],
        ["2026-05-11(월)", "200", "이영희", "결제취소-카드", 30000, ""],
    ))
    table = app.compute_period_recon(hansol, patient)
    tabs = {
        date(2026, 5, 11): F.table_raw(
            ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
            [1, "100", "김철수", "", 50000, 0, 0],
            [2, "200", "이영희", "", 30000, 0, 0],
            [3, "200", "이영희", "환불", 30000, 0, 0],
        ),
    }
    t2, skipped, _ = app.augment_period_with_gsheet(
        table, patient, "sheet-id", loader=_fake_gsheet_loader(tabs))
    d1 = t2[t2["날짜"] == "2026-05-11"].iloc[0]
    assert not skipped
    assert d1["일마카드"] == 50000                       # 80,000 - 환불 30,000
    assert d1["일마-차트카드차이"] == 0


# ── 하루 모드의 다일 파일 자동 필터 ──────────────────────────

def test_filter_to_single_date_picked_date(app):
    hansol, patient = _two_day_files(app)
    daily = pd.DataFrame({"차트번호": ["100", "200"]})
    p2, h2, note, err = app.filter_to_single_date(
        patient, hansol, daily, picked_date=date(2026, 5, 11))
    assert err == ""
    assert set(p2["날짜"]) == {"2026-05-11"} and set(h2["날짜"]) == {"2026-05-11"}
    assert "2026-05-11" in note


def test_filter_to_single_date_overlap_heuristic(app):
    """파일 업로드 일마(날짜 미지정) → 차트번호 겹침 최대 수납일 자동 선택."""
    hansol, patient = _two_day_files(app)
    daily = pd.DataFrame({"차트번호": ["300", "400"]})    # 5/12 환자들
    p2, h2, note, err = app.filter_to_single_date(patient, hansol, daily)
    assert err == ""
    assert set(p2["날짜"]) == {"2026-05-12"} and set(h2["날짜"]) == {"2026-05-12"}


def test_filter_to_single_date_missing_date_errors(app):
    hansol, patient = _two_day_files(app)
    daily = pd.DataFrame({"차트번호": ["100"]})
    _, _, _, err = app.filter_to_single_date(
        patient, hansol, daily, picked_date=date(2026, 7, 1))
    assert err != ""                                       # 파일에 없는 날짜 → 오류


def test_filter_to_single_date_single_day_noop(app):
    """하루치 차트 + 하루치 한솔이면 필터 없이 그대로(기존 동작 보존)."""
    hansol, _, patient_raw = F.basic_three_files()
    h = app.parse_hansol(hansol)
    p = app.parse_patient(patient_raw)
    daily = pd.DataFrame({"차트번호": ["100", "200", "300"]})
    p2, h2, note, err = app.filter_to_single_date(p, h, daily)
    assert err == "" and len(p2) == len(p) and len(h2) == len(h)

"""데이터 검증(차트↔일마 유형B/C1/C2) + 전각숫자 정규화 회귀 테스트.

실데이터(인천 5/22 김은정·유지윤, 엔디어트 6/1 신이재·박윤수)에서 발견된
검증 가짜 양성·매칭 누락 사례를 고정한다.
"""
import factories as F


def test_clean_no_fullwidth_digits(app):
    """차트 결제메모의 전각 승인번호(１５６０１１００)도 반각으로 정규화."""
    assert app.clean_no("１５６０１１００") == "15601100"
    assert app.clean_no("23206298") == "23206298"
    assert app.clean_no("") == ""


def test_fullwidth_memo_approval_matches_hansol(app):
    """전각 메모 승인번호가 한솔(반각)과 기간 대사에서 매칭돼야 함(엔디어트 박윤수 사례)."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[559000],
        승인번호=["15601100"],
        거래일=["260609"],
        거래시간=["193046"],
        거래상태=["정상승인"],
        구분=["카드"],
        매입사=["하나카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["수납일", "차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["2026-06-09(화)", "14588", "박윤수", "카드(구분안함)", 559000, "１５６０１１００"],
    ))
    un_h, un_p = app.find_period_day_detail(hansol, patient, "2026-06-09")
    assert un_h.empty and un_p.empty


def _verif_inputs(app, with_refund=True):
    """당일 결제 후 당일 환불(net 0) 환자 시나리오 (인천 5/22 유지윤 사례)."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["920", "유지윤", "카드(국민)", 329000, "26470304"],
        ["920", "유지윤", "결제취소-카드", 329000, "30018692"],
        ["100", "김철수", "카드(삼성)", 50000, ""],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "920", "유지윤", "구환", 329000, 0, 0],
        [2, "920", "유지윤", "환불", 329000, 0, 0],
        [3, "100", "김철수", "구환", 50000, 0, 0],
    ))
    return patient, daily, (refund if with_refund else None)


def test_verification_nets_daily_refund(app):
    """일마 환불 행을 차감하면 net 0 환자가 유형C2로 잡히지 않아야 함."""
    patient, daily, refund = _verif_inputs(app)
    assert not refund.empty                       # 환불 행 분리 확인
    verif = app.build_verification(patient, daily, refund)
    assert verif["유형C2_금액불일치"].empty
    assert verif["유형C1_한쪽만존재"].empty
    assert verif["유형B_차트번호오타"].empty


def test_verification_without_refund_kept_backward_compat(app):
    """daily_refund 없이 호출하면(기존 시그니처) 종전과 동일하게 동작."""
    patient, daily, _ = _verif_inputs(app, with_refund=False)
    verif = app.build_verification(patient, daily)
    # 환불 미반영 → 유지윤이 금액불일치로 잡힘 (종전 동작 보존 확인)
    assert len(verif["유형C2_금액불일치"]) == 1


def test_verification_c3_method_mismatch(app):
    """총액은 같지만 결제수단 분배가 다른 환자(채널 합계 차이의 직접 원인)를
    유형C3로 확정 추출. 총액이 같으므로 C2엔 잡히지 않아야 함."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드(삼성)", 50000, ""],     # 차트: 카드 50,000
        ["200", "이영희", "현금영수증", 30000, ""],      # 정상 환자 (분배 일치)
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "100", "김철수", "", 0, 50000, 0],           # 일마: 현금 50,000 (오기재)
        [2, "200", "이영희", "", 0, 30000, 0],
    ))
    verif = app.build_verification(patient, daily, refund)
    assert verif["유형C2_금액불일치"].empty
    c3 = verif["유형C3_결제수단불일치"]
    assert len(c3) == 1
    r = c3.iloc[0]
    assert r["차트번호"] == "100" and r["총액"] == 50000
    assert "카드↔현금" in r["추정원인"]


def test_verification_c1_same_amount_hint(app):
    """한쪽만존재 양쪽에 동일 금액 1건씩 남으면 '동일건 의심' 힌트로 연결."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드(삼성)", 77000, ""],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "9999", "박오기", "", 77000, 0, 0],          # 이름·번호 모두 오기재
    ))
    verif = app.build_verification(patient, daily, refund)
    c1 = verif["유형C1_한쪽만존재"]
    assert len(c1) == 2
    hints = list(c1["동일금액상대"])
    assert any("동일건 의심" in h for h in hints)


def test_ai_text_verification_includes_c3(app):
    """AI 입력의 [데이터검증] 섹션에 유형C3(결제수단불일치)가 포함돼야 함."""
    import pandas as pd
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["100", "김철수", "카드(삼성)", 50000, ""],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "100", "김철수", "", 0, 50000, 0],
    ))
    verif = app.build_verification(patient, daily, refund)
    totals = app.compute_totals(pd.DataFrame(), daily, refund, patient)
    channel = app.compute_channel_recon(totals)
    text = app.build_ai_text(pd.DataFrame(), daily, refund, patient, channel,
                             None, None, None, {}, totals=totals, verif=verif)
    assert "[데이터검증" in text
    assert "유형C3" in text and "김철수" in text


def _refund_double_deduction_inputs(app):
    """잠실 7/15 조인태 실사례 축소판.

    차트(EMR): 카드 396,000 결제 → 전액취소(-396,000) → 재승인 341,000 = net 341,000.
    일일마감: 본행 카드 341,000(이미 환불 반영된 재승인액) + 환불행 55,000 또 차감
              = net 286,000 (환불 이중차감 오류).
    한솔(PG): 341,000 승인 1건 = 차트와 일치 → 일일마감 오류 확정.
    """
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["30366", "조인태", "카드(현대)", 396000, ""],
        ["30366", "조인태", "환불-카드", 396000, ""],
        ["30366", "조인태", "카드(재승인)", 341000, "406318"],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "30366", "조인태", "구환", 341000, 0, 0],
        [2, "30366", "조인태", "환불", 55000, 0, 0],
    ))
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[341000], 승인번호=["406318"], 거래시간=["182751"],
        거래상태=["정상승인"], 구분=["카드"], 매입사=["현대카드"],
    ))
    return patient, daily, refund, hansol


def test_verification_refund_double_deduction_diagnosed(app):
    """일마 환불 이중차감(잠실 7/15 조인태 유형)이 유형C2 추정원인으로 진단돼야 함."""
    patient, daily, refund, _ = _refund_double_deduction_inputs(app)
    verif = app.build_verification(patient, daily, refund)
    c2 = verif["유형C2_금액불일치"]
    assert len(c2) == 1
    r = c2.iloc[0]
    assert r["차트번호"] == "30366"
    assert r["차트금액"] == 341000 and r["일마금액"] == 286000 and r["차이"] == -55000
    assert "환불" in r["추정원인"] and "55,000" in r["추정원인"]
    assert "이중차감" in r["추정원인"]


def test_verification_refund_hansol_verdict(app):
    """한솔(PG)을 주면 차트↔일마 중 어느 파일이 틀렸는지 3자 판정까지 제시."""
    patient, daily, refund, hansol = _refund_double_deduction_inputs(app)
    verif = app.build_verification(patient, daily, refund, hansol=hansol)
    r = verif["유형C2_금액불일치"].iloc[0]
    assert "차트마감 일치" in r["한솔판정"]
    assert "일일마감 쪽 오류" in r["한솔판정"]


def test_verification_hansol_verdict_fallback_without_approval_link(app):
    """실제 잠실 7/15 조건: 차트 결제메모에 승인번호가 없어도(미링크)
    한솔 승인금액 존재 여부로 정황 판정이 나와야 함."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["30366", "조인태", "카드(현대)", 396000, "마취크림 환불로 수가 넣음"],
        ["30366", "조인태", "환불-카드", 396000, "마취크림 환불로 수가 넣음"],
        ["30366", "조인태", "카드(재승인)", 341000, ""],   # 승인번호 없음
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "30366", "조인태", "구환", 341000, 0, 0],
        [2, "30366", "조인태", "환불", 55000, 0, 0],
    ))
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[341000], 승인번호=["406318"], 거래시간=["182751"],
        거래상태=["정상승인"], 구분=["카드"], 매입사=["현대카드"],
    ))
    verif = app.build_verification(patient, daily, refund, hansol=hansol)
    r = verif["유형C2_금액불일치"].iloc[0]
    assert "차트마감 지지" in r["한솔판정"] and "정황" in r["한솔판정"]


def test_verification_refund_missing_in_daily(app):
    """반대 방향: 차트에는 환불이 반영됐는데 일마에 환불행이 누락된 경우."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["700", "박환불", "카드(국민)", 300000, ""],
        ["700", "박환불", "환불-카드", 50000, ""],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "700", "박환불", "구환", 300000, 0, 0],   # 환불행 없음 → 일마 과대
    ))
    verif = app.build_verification(patient, daily, refund)
    c2 = verif["유형C2_금액불일치"]
    assert len(c2) == 1
    r = c2.iloc[0]
    assert r["차이"] == 50000
    assert "일마 환불 미반영" in r["추정원인"]


def test_hansol_card_by_chart_nets_cancels(app):
    """한솔 취소 건이 승인번호 링크 시 음수로 차감돼 net으로 귀속돼야 함."""
    hansol = app.parse_hansol(F.hansol_raw(
        금액=[396000, 396000, 341000],
        승인번호=["111222", "111222", "406318"],
        거래시간=["100000", "110000", "120000"],
        거래상태=["정상승인", "취소승인", "정상승인"],
        구분=["카드", "카드", "카드"],
        매입사=["현대카드", "현대카드", "현대카드"],
    ))
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["30366", "조인태", "카드(현대)", 396000, "111222"],
        ["30366", "조인태", "환불-카드", 396000, "111222"],
        ["30366", "조인태", "카드(재승인)", 341000, "406318"],
    ))
    m = app._hansol_card_by_chart(hansol, patient)
    # 정상 396,000 - 취소 396,000 + 정상 341,000 = 341,000 (차트 net과 일치)
    assert m["30366"][0] == 341000


def test_ai_text_c2_includes_refund_cause(app):
    """AI 입력 유형C2 섹션에 환불 진단(원인)·한솔판정 컬럼이 실려야 함."""
    import pandas as pd
    patient, daily, refund, hansol = _refund_double_deduction_inputs(app)
    verif = app.build_verification(patient, daily, refund, hansol=hansol)
    totals = app.compute_totals(hansol, daily, refund, patient)
    channel = app.compute_channel_recon(totals)
    text = app.build_ai_text(hansol, daily, refund, patient, channel,
                             None, None, None, {}, totals=totals, verif=verif)
    assert "이중차감" in text
    assert "한솔판정" in text


def test_verification_refund_only_daily_patient(app):
    """일마에 환불만 있는 환자(net 음수)도 차트 음수 행과 대조돼 일치 처리."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모"],
        ["500", "이전결제", "결제취소-카드", 100000, ""],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체"],
        [1, "500", "이전결제", "환불", 100000, 0, 0],
    ))
    verif = app.build_verification(patient, daily, refund)
    assert verif["유형C2_금액불일치"].empty
    assert verif["유형C1_한쪽만존재"].empty

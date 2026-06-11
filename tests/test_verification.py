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


def test_verification_carries_staff_columns(app):
    """수납자(차트)·담당(일마) 정보가 검증 결과에 실려 '누구를 검토할지' 추적 가능."""
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모", "수납자"],
        ["100", "김철수", "카드(삼성)", 50000, "", "박직원"],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체", "담당/결제"],
        [1, "100", "김철수", "", 55000, 0, 0, "이프론트"],
    ))
    verif = app.build_verification(patient, daily, refund)
    c2 = verif["유형C2_금액불일치"]
    assert len(c2) == 1
    r = c2.iloc[0]
    assert r["차트수납자"] == "박직원" and r["일마담당"] == "이프론트"


def test_ai_text_verification_includes_staff(app):
    """AI 입력의 [데이터검증] 섹션에 담당자(차트/일마)가 인용돼 R10 권고가 가능."""
    import pandas as pd
    patient = app.parse_patient(F.table_raw(
        ["차트번호", "이름", "결제수단", "비급여(과세총금액)", "결제메모", "수납자"],
        ["100", "김철수", "카드(삼성)", 50000, "", "박직원"],
    ))
    daily, refund = app.parse_daily(F.table_raw(
        ["내원순서", "차트번호", "성명", "구분", "카드", "현금", "이체", "담당/결제"],
        [1, "100", "김철수", "", 55000, 0, 0, "이프론트"],
    ))
    verif = app.build_verification(patient, daily, refund)
    totals = app.compute_totals(pd.DataFrame(), daily, refund, patient)
    channel = app.compute_channel_recon(totals)
    text = app.build_ai_text(pd.DataFrame(), daily, refund, patient, channel,
                             None, None, None, {}, totals=totals, verif=verif)
    assert "[데이터검증" in text
    assert "박직원/이프론트" in text


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

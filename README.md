# 병원 정산 3-Way 대사 시스템 (Streamlit)

한솔페이 / 일일마감 / 차트마감 데이터를 비교하여 결제수단별 차이를 분석합니다.

## 실행 방법

```bash
pip install -r requirements.txt
streamlit run app.py
```

## 일일마감 구글시트 연동 (선택)

일일마감은 파일 업로드 대신 **지점별 구글 스프레드시트**에서 바로 읽어올 수 있습니다.
일일마감 입력란에서 `구글시트 연동`을 선택 → **지점 드롭다운** + **달력**으로 날짜를 고르면,
해당 날짜 이름의 워크시트(탭)를 읽어 분석합니다(파일 업로드 불필요).

설정 방법:

1. 각 지점의 일일마감 스프레드시트를 `[공유] → "링크가 있는 모든 사용자" → 뷰어`로 변경
   (별도 인증키/서비스계정 불필요).
2. `app.py` 상단의 `CLINIC_DAILY_SHEETS` 에 `{지점명: 스프레드시트 URL}` 입력.
   URL을 깃에 남기기 싫으면 `.streamlit/secrets.toml` 의 `[clinic_daily_sheets]` 섹션에 적으면
   그 값이 우선 적용됩니다.
3. 각 스프레드시트의 **탭 이름은 날짜** 형식이어야 합니다. 기본은 `26.06.02`(= `%y.%m.%d`)이며,
   다르면 `app.py` 의 `DAILY_SHEET_DATE_FMT` 를 바꾸면 됩니다.

> 시트가 비공개(특정 계정만 공유)면 이 방식으로는 읽을 수 없으며, 접근 권한·시트 이름 안내
> 메시지가 표시됩니다.

## 테스트

파서·매칭·합계대사·산출물의 정확도를 회귀 검증하는 pytest 스위트가 `tests/`에 있습니다.

```bash
pip install -r requirements-dev.txt
pytest
```

- `tests/conftest.py` — streamlit 경량 스텁 주입(설치 없이도 순수 로직 테스트 가능).
- `tests/factories.py` — 합성 원본 데이터(한솔/일마/차트) 빌더.
- `test_utils` / `test_parsers` / `test_matching` / `test_reconciliation` / `test_outputs`
  — 시간 파싱·결제수단 분류·승인번호 매칭·환불 net 처리·★★ 확정매칭·엑셀 산출 등을 검증.

> UI 로직은 `app.py`의 `main()` 안에 있어 `import app` 시 실행되지 않습니다(import-safe).
> `streamlit run app.py`는 `__name__ == "__main__"`에서 `main()`을 호출합니다.

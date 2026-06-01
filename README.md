# 병원 정산 3-Way 대사 시스템 (Streamlit)

한솔페이 / 일일마감 / 차트마감 데이터를 비교하여 결제수단별 차이를 분석합니다.

## 실행 방법

```bash
pip install -r requirements.txt
streamlit run app.py
```

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

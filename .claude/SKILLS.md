# Claude Code 스킬 구성

이 저장소의 스킬은 전부 공용 마켓플레이스
[`vslineclinic/claude-skills`](https://github.com/vslineclinic/claude-skills)에서
옵니다. 스킬 파일 사본은 이 저장소에 두지 않습니다. 구성은 `settings.json`의
`extraKnownMarketplaces`와 `enabledPlugins` 두 항목뿐입니다.

스킬을 추가·수정·업데이트하려면 이 저장소가 아니라 마켓플레이스 저장소를
고쳐야 합니다.

## 켜져 있는 플러그인

| 분류 | 플러그인 | 스킬 수 |
| --- | --- | --- |
| 문서·범용 | `example-skills` | 12 |
| 문서·범용 | `document-skills` | 4 |
| SEO | `claude-seo` | 33 |
| SEO | `rampstack-seo` | 12 |
| 광고 | `claude-ads` | 34 |
| 마케팅 | `marketing-skills` | 49 |
| 금융 | `financial-analysis` | 13 |
| 금융 | `core` | 3 |
| 기업 업무 | `corporate-skills` | 166 |
| 법무 | `legal-skills` | 14 |

스킬은 플러그인 이름으로 네임스페이스가 붙습니다
(예: `claude-seo:seo-audit`, `corporate-skills:finance-dcf-model`).

## 처음 세션을 열 때

마켓플레이스 플러그인은 이 저장소에 포함되지 않고 각자의 머신에 내려받아집니다.
그래서 처음에는 설치 확인 프롬프트가 뜨거나 아래 명령이 안내됩니다.

```
claude plugin install <plugin>@vslineclinic --scope project
```

`vslineclinic/claude-skills`는 **비공개 저장소**입니다. 로컬 터미널에서는 각자의
git 자격증명으로 바로 받아지지만, Claude Code on the web 세션에서 쓰려면 그
환경에 해당 저장소 접근 권한이 있어야 합니다. 없으면 플러그인이 조용히 로드되지
않습니다.

## 컨텍스트 비용

스킬 이름과 설명은 매 세션 시작 시 컨텍스트에 올라갑니다. 위 10개를 모두 켜면
스킬 약 318개, 설명만 대략 25~30k 토큰입니다. 무거우면 `settings.json`의
`enabledPlugins`에서 값을 `false`로 바꾸거나 줄을 지우면 분류별로 정리됩니다.

`document-skills`의 docx/pdf/pptx/xlsx와 `corporate-skills`의 `docs-*`는 Claude
Code 기본 제공 스킬과 기능이 겹칩니다. 줄일 때 먼저 후보로 보세요.

clinic-pay는 결제 서비스 저장소라 실제로 필요한 분류는 제한적일 수 있습니다.
켜 둔 10개는 처음 요청대로 전부 유지한 상태입니다.

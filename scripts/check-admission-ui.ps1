param(
  [string]$Page = "ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/pages/oxidelog/index.tsx",
  [string]$Service = "ant-design-pro-6.0.1/ant-design-pro-6.0.1/src/services/oxidelog.ts"
)
$ErrorActionPreference = "Stop"
$pageText = Get-Content -LiteralPath $Page -Raw -Encoding UTF8
$serviceText = Get-Content -LiteralPath $Service -Raw -Encoding UTF8
$requiredPage = @(
  "key: 'admission'",
  "准入控制",
  "renderAdmission",
  "approveAdmissionCase",
  "loadAdmission"
)
$requiredService = @(
  "AdmissionCase",
  "admissionCases",
  "approveAdmissionCase",
  "blockAdmissionCase",
  "reopenAdmissionCase",
  "admissionProfiles"
)
$missing = @()
foreach ($needle in $requiredPage) {
  if (-not $pageText.Contains($needle)) { $missing += "page:$needle" }
}
foreach ($needle in $requiredService) {
  if (-not $serviceText.Contains($needle)) { $missing += "service:$needle" }
}
if ($missing.Count -gt 0) {
  Write-Error ("Missing admission Ant UI markers: " + ($missing -join ', '))
}

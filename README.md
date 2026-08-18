warning: The `fitz` API is deprecated and will be removed in future. Use `import pymupdf` instead.
# Historical exam dates CKA, CKAD & CKS

Helps prepare for a Kubernetes exam by estimating when an exam will switch to a new Kubernetes version.

Current exam versions can be found in [FAQ CKA, CKAD & CKS](https://docs.linuxfoundation.org/tc-docs/certification/faq-cka-ckad-cks#what-application-version-is-running-in-the-exam-environment)

### [CKA](https://training.linuxfoundation.org/certification/certified-kubernetes-administrator-cka/)

| K8s  | K8s GA      | CKA Switch  | Day  | Days | Overdue|
|:-----|:------------|:------------|:----:|:----:|:------:|
| 1.37 | ~2026-08-26 | ~2026-11-03 | ~Tue |  ~69 |        |
| 1.36 | 2026-04-22  | ~2026-06-30 | ~Tue |  ~69 | ~49   |
| 1.35 | 2025-12-17  | 2026-03-03  | Tue  |   76 |        |
| 1.34 | 2025-08-27  | 2025-10-28  | Tue  |   62 |        |
| 1.33 | 2025-04-23* | 2025-07-03  | Thu  |   71 |        |
| 1.32 | 2024-12-11* | 2025-02-17 ¹| Mon  |   68 |        |
| 1.31 | 2024-08-13* | 2024-10-01  | Tue  |   49 |        |
| 1.30 | 2024-04-17* | 2024-05-28  | Tue  |   41 |        |

~ Predicted: K8s GA + 69d avg (v1.32–v1.35), nearest Tue<br>
¹ v1.31 → v1.32 topics changed: [v1.31 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKA_Curriculum_v1.31.pdf) · [v1.32 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKA_Curriculum_v1.32.pdf)

### [CKAD](https://training.linuxfoundation.org/certification/certified-kubernetes-application-developer-ckad/)

| K8s  | K8s GA      | CKAD Switch | Day  | Days | Overdue|
|:-----|:------------|:------------|:----:|:----:|:------:|
| 1.37 | ~2026-08-26 | ~2026-10-27 | ~Tue |  ~62 |        |
| 1.36 | 2026-04-22  | ~2026-06-23 | ~Tue |  ~62 | ~56   |
| 1.35 | 2025-12-17  | 2026-02-25  | Wed  |   70 |        |
| 1.34 | 2025-08-27  | 2025-10-20  | Mon  |   54 |        |
| 1.33 | 2025-04-23* | 2025-06-18 ²| Wed  |   56 |        |
| 1.32 | 2024-12-11* | 2025-02-18 ³⁴| Tue  |   69 |        |
| 1.31 | 2024-08-13* | 2024-10-15  | Tue  |   63 |        |
| 1.30 | 2024-04-17* | 2024-05-21  | Tue  |   34 |        |

~ Predicted: K8s GA + 62d avg (v1.32–v1.35), nearest Tue<br>
² v1.32 → v1.33 topics changed: Removed: Kuztomize · Added: Kustomize<br>
³ v1.31 → v1.32 topics changed: [v1.31 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKAD_Curriculum_v1.31.pdf) · [v1.32 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKAD_Curriculum_v1.32.pdf)<br>
⁴ v1.32 curriculum revised 2025-05-19: fixed minor typos: De~~velo~~ployment. Understand~~ing~~ requests, limits, and quotas. Understand Application -> Understand Application Security.

### [CKS](https://training.linuxfoundation.org/certification/certified-kubernetes-security-specialist/)

| K8s  | K8s GA      | CKS Switch  | Day  | Days | Overdue|
|:-----|:------------|:------------|:----:|:----:|:------:|
| 1.37 | ~2026-08-26 | ~2026-11-03 | ~Tue |  ~69 |        |
| 1.36 | 2026-04-22  | ~2026-06-30 | ~Tue |  ~69 | ~49   |
| 1.35 | 2025-12-17  | ~2026-05-28 ⁷| ~Thu | ~162 |        |
| 1.34 | 2025-08-27  | 2025-10-30  | Thu  |   64 |        |
| 1.33 | 2025-04-23* | 2025-07-03 ⁵| Thu  |   71 |        |
| 1.32 | 2024-12-11* | 2025-02-25 ⁶| Tue  |   76 |        |
| 1.31 | 2024-08-13* | 2024-10-15  | Tue  |   63 |        |
| 1.30 | 2024-04-17* | 2024-06-11  | Tue  |   55 |        |

~ Predicted: K8s GA + 68d avg (v1.31–v1.34), nearest Tue<br>
⁵ v1.32 → v1.33 topics changed: [v1.32 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKS_Curriculum%20v1.32.pdf) · [v1.33 curriculum](https://github.com/cncf/curriculum/blob/master/old-versions/CKS_Curriculum%20v1.33.pdf)<br>
⁶ v1.32 curriculum revised 2025-04-08: Added *Istio* to: Implement Pod-to-Pod encryption (Cilium, Istio)<br>
⁷ v1.35 switch date estimated via [FAQ CKA, CKAD & CKS](https://docs.linuxfoundation.org/tc-docs/certification/faq-cka-ckad-cks#what-application-version-is-running-in-the-exam-environment); CNCF has not published a curriculum PDF

\* EOL (end of life)

<!-- footer -->
[![Last updated](https://img.shields.io/github/last-commit/smartbit/cka-ckad-cks-release-tracker?style=for-the-badge&label=updated)](https://github.com/smartbit/cka-ckad-cks-release-tracker/commits/main)
[![Tests](https://img.shields.io/github/actions/workflow/status/smartbit/cka-ckad-cks-release-tracker/daily.yml?branch=main&style=for-the-badge&logo=github&label=Tests)](https://github.com/smartbit/cka-ckad-cks-release-tracker/actions/workflows/daily.yml)
[![Python](https://img.shields.io/badge/python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)
[![License](https://img.shields.io/github/license/smartbit/cka-ckad-cks-release-tracker?style=for-the-badge)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/smartbit/cka-ckad-cks-release-tracker?style=for-the-badge&logo=github&color=181717)](https://github.com/smartbit/cka-ckad-cks-release-tracker)

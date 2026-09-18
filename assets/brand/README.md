# Solviva brand assets

Source of truth on Alden's machine: `C:\Users\roald\Pictures\Solviva` (copied here
2026-09-18 so the repo and its deploys are self-contained). Apply these to every
Solviva-facing page or document; do not invent colours or redraw the mark.

## Logo

| File | What | Use |
|---|---|---|
| `solviva-mark-green.png` | Sunburst mark, dark green on transparent, 232×232 | Favicon/app icon on light backgrounds |
| `solviva-tile-lime.jpg` | Dark green mark on lime square, 320×320 | App-icon tile; header logo on the dark green bar |
| `solviva-logo-square-lime.jpg` | Mark + wordmark + "An AboitizPower Company", lime square, 200×200 | Social / square placements |
| `solviva-logo-wordmark-green.jpg` | Mark + wordmark, dark green on white, 278×80 | Login screens, documents, light backgrounds |
| `solviva-logo-wordmark-lime.jpg` | Mark + wordmark, lime on white, 276×80 | Only on dark backgrounds after cutting out the white |
| `website-css-variables.png` | Screenshot of solvivaenergy.com's `:root` variables | Palette reference below |

Always "SOLVIVA" with the sunburst; the tagline is "An AboitizPower Company".

## Palette (from solvivaenergy.com `:root`)

| Token on the website | Hex | Role in our UIs |
|---|---|---|
| `--primary-dark` | `#1f512a` | Brand dark green: headers, primary buttons, links, KPI numbers |
| `--primary-dark` (alt) | `#50890a` | Mid green: hover state of primary, "ok" states |
| `--primary-light` | `#d2ff1e` | Lime accent: active tab, accent bars, highlights — dark green text on it, never white |
| `--secondary-dark` | `#006ac6` | Blue: informational tags/links when green would be ambiguous |
| `--secondary-key` | `#00a7ea` | Light blue: charts, secondary accents |
| `--neutral-dark` | `#212121` | Body text |
| `--neutral-light` | `#fcfcfc` | Page/card background |
| `--neutral-mid-1` | `#e4e7ec` | Borders, dividers |
| `--neutral-mid-2` | `#78909c` | Muted icons |
| `--untitled-ui--gray500` | `#667085` | Muted text |
| `--untitled-ui--gray600` | `#475467` | Secondary text |
| `--untitled-ui--gray900` | `#101828` | Headings on light |
| `--untitled-ui--gray300` | `#d0d5dd` | Disabled borders |

Contrast notes: `#1f512a` on white passes AA for all text sizes; `#d2ff1e` is a
background colour only (pair with `#1f512a`); `#50890a` on white passes AA for
normal text but not as a fill behind white text.

## Where it is applied

- `api/monitoring_admin/index.html` — Monitoring Admin (tokens in `:root`,
  brand bar `#topbar`, login wordmark, favicon tile as data URIs).

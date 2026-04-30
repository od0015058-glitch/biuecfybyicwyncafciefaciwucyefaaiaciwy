# Favicon / App Icon

## Context for the AI image generator

This icon must be recognizable at very small sizes (down to 16×16 pixels).
Meowassist is a Telegram AI bot with a cat mascot.

---

## PROMPT — Favicon (512×512, will be downscaled to 32×32 and 16×16)

Extremely simplified version of the Meowassist cat logo. Only the essential
shapes: a cat ear silhouette pointing upward with one small glowing dot
representing the AI eye (electric cyan #00E5FF). Deep purple (#6C3CE1)
background. Rounded square shape with iOS-style rounded corners (about 22%
corner radius). The design must be recognizable and distinct even when scaled
down to 16×16 pixels — test by squinting at the result. No text, no fine
details, no thin lines. Bold shapes only. The cat ear + glowing eye should
be centered within the rounded square. Think: if you blur the icon to 8×8
pixels, you should still see "purple square with a cyan dot and a pointy ear."

---

## PROMPT — Alternative: Minimal cat silhouette (512×512)

Even simpler: just the outline of a cat head (two pointed ears, rounded face)
in white on deep purple (#6C3CE1) rounded square background. One small cyan
(#00E5FF) dot for the eye. Absolutely no detail — pure silhouette. Must work
at 16×16 without any artifacts.

---

## Usage notes

After generating, export at these sizes:
- `favicon-512.png` — 512×512 (master)
- `favicon-192.png` — 192×192 (Android Chrome)
- `favicon-32.png` — 32×32 (browser tab)
- `favicon-16.png` — 16×16 (smallest browser tab)
- `favicon.ico` — multi-resolution ICO file containing 16+32+48

Use a tool like https://realfavicongenerator.net to generate all sizes
from the 512×512 master.

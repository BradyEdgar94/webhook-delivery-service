#!/usr/bin/env python3
"""Draws docs/images/autoscaling-{light,dark}.svg from the measured kind run.

Reads the timeline table in docs/results/2026-11-20-k8s-autoscaling.md, so the
chart cannot drift from the data. Run: python3 docs/images/build_autoscaling_chart.py

Two stacked panels on one time axis (consumer lag, worker replicas) rather
than one chart with two y-axes: the two measures have very different scales.
"""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, "..", "results", "2026-11-20-k8s-autoscaling.md")

rows = []
for line in open(SOURCE):
    m = re.match(r"\|\s*(\d+)\s*\|\s*(\w+)\s*\|\s*(\d+)/(\d+)\s*\|\s*(\d+)\s*\|", line)
    if m:
        rows.append((int(m[1]), m[2], int(m[4]), int(m[5])))  # t, k6 state, desired workers, lag

THEMES = {
    "light": dict(surface="#fcfcfb", text="#0b0b0b", muted="#52514e", grid="#e6e5e1", series="#2a78d6"),
    "dark": dict(surface="#1a1a19", text="#f0efec", muted="#c3c2b7", grid="#33322f", series="#3987e5"),
}

W, H = 860, 500
LEFT, RIGHT = 64, 28
PANELS = [  # (top, height, title, y max, ticks)
    (96, 150, "Kafka consumer lag (messages waiting)", 25000, [0, 10000, 20000]),
    (300, 150, "Worker replicas (set by KEDA)", 12, [0, 4, 8, 12]),
]
T_MAX = 100
FONT = "font-family=\"-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif\""


def x(t):
    return LEFT + (W - LEFT - RIGHT) * t / T_MAX


def y(v, top, height, vmax):
    return top + height - height * v / vmax


def fmt(n):
    return f"{n // 1000}K" if n >= 10000 else f"{n:,}"


def draw(theme):
    c = THEMES[theme]
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
         f'aria-label="Workers scale from 1 to 11 as Kafka consumer lag grows to 21,143, then back to 1 after it drains">',
         f'<rect width="{W}" height="{H}" fill="{c["surface"]}"/>',
         f'<text x="{LEFT}" y="28" {FONT} font-size="16" font-weight="600" fill="{c["text"]}">'
         'Workers scale out on consumer lag, then back in</text>',
         f'<text x="{LEFT}" y="47" {FONT} font-size="12" fill="{c["muted"]}">'
         'Local Kubernetes (kind) with KEDA, 3,000 events/s burst. All 77,634 events delivered.</text>']

    burst_end = next(t for t, state, _, _ in rows if state == "done")
    for top, height, title, vmax, ticks in PANELS:
        o.append(f'<text x="{LEFT}" y="{top - 8}" {FONT} font-size="12" font-weight="600" fill="{c["text"]}">{title}</text>')
        for tick in ticks:
            yy = y(tick, top, height, vmax)
            o.append(f'<line x1="{LEFT}" x2="{W - RIGHT}" y1="{yy:.1f}" y2="{yy:.1f}" stroke="{c["grid"]}" stroke-width="1"/>')
            o.append(f'<text x="{LEFT - 8}" y="{yy + 4:.1f}" {FONT} font-size="11" text-anchor="end" fill="{c["muted"]}">{fmt(tick)}</text>')
        # when the burst stopped
        bx = x(burst_end)
        o.append(f'<line x1="{bx:.1f}" x2="{bx:.1f}" y1="{top}" y2="{top + height}" stroke="{c["muted"]}" stroke-width="1"/>')

    # panel 1: lag as a line with a light wash underneath
    top, height, _, vmax, _ = PANELS[0]
    pts = [(x(t), y(lag, top, height, vmax)) for t, _, _, lag in rows]
    line = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
    base = top + height
    o.append(f'<polygon points="{pts[0][0]:.1f},{base} {line} {pts[-1][0]:.1f},{base}" fill="{c["series"]}" fill-opacity="0.10"/>')
    o.append(f'<polyline points="{line}" fill="none" stroke="{c["series"]}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
    peak_t, _, _, peak_lag = max(rows, key=lambda r: r[3])
    px, py = x(peak_t), y(peak_lag, top, height, vmax)
    o.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="5" fill="{c["series"]}" stroke="{c["surface"]}" stroke-width="2"/>')
    o.append(f'<text x="{px - 10:.1f}" y="{py - 8:.1f}" {FONT} font-size="12" text-anchor="end" fill="{c["text"]}">peak {peak_lag:,}</text>')
    bx = x(burst_end)
    o.append(f'<text x="{bx + 6:.1f}" y="{top + 12}" {FONT} font-size="11" fill="{c["muted"]}">burst ends</text>')
    zero_t = next(t for t, _, _, lag in rows if lag == 0 and t > peak_t)
    o.append(f'<text x="{x(zero_t) + 6:.1f}" y="{base - 8}" {FONT} font-size="12" fill="{c["text"]}">drained at {zero_t} s</text>')

    # panel 2: replicas as a step line (the count holds until KEDA changes it)
    top, height, _, vmax, _ = PANELS[1]
    step = []
    prev = None
    for t, _, workers, _ in rows:
        if prev is not None:
            step.append((x(t), y(prev, top, height, vmax)))
        step.append((x(t), y(workers, top, height, vmax)))
        prev = workers
    line = " ".join(f"{px:.1f},{py:.1f}" for px, py in step)
    base = top + height
    o.append(f'<polygon points="{step[0][0]:.1f},{base} {line} {step[-1][0]:.1f},{base}" fill="{c["series"]}" fill-opacity="0.10"/>')
    o.append(f'<polyline points="{line}" fill="none" stroke="{c["series"]}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
    peak_workers = max(w for _, _, w, _ in rows)
    first_peak = next(t for t, _, w, _ in rows if w == peak_workers)
    last_peak = max(t for t, _, w, _ in rows if w == peak_workers)
    mid = x((first_peak + last_peak) / 2)
    o.append(f'<text x="{mid:.1f}" y="{y(peak_workers, top, height, vmax) - 8:.1f}" {FONT} font-size="12" text-anchor="middle" fill="{c["text"]}">{peak_workers} workers</text>')
    o.append(f'<text x="{x(rows[0][0]) + 4:.1f}" y="{y(1, top, height, vmax) - 8:.1f}" {FONT} font-size="12" fill="{c["text"]}">1</text>')
    last_t = rows[-1][0]
    o.append(f'<text x="{x(last_t):.1f}" y="{y(5, top, height, vmax) - 8:.1f}" {FONT} font-size="12" text-anchor="end" fill="{c["text"]}">back to 1 at {last_t} s</text>')

    # shared time axis
    axis_y = PANELS[1][0] + PANELS[1][1]
    for t in range(0, T_MAX + 1, 20):
        o.append(f'<text x="{x(t):.1f}" y="{axis_y + 18}" {FONT} font-size="11" text-anchor="middle" fill="{c["muted"]}">{t} s</text>')
    o.append(f'<text x="{W - RIGHT}" y="{H - 10}" {FONT} font-size="11" text-anchor="end" fill="{c["muted"]}">'
             'Source: docs/results/2026-11-20-k8s-autoscaling.md, sampled every 5 s</text>')
    o.append("</svg>")
    out = os.path.join(HERE, f"autoscaling-{theme}.svg")
    with open(out, "w") as f:
        f.write("\n".join(o) + "\n")
    print("wrote", out)


for name in THEMES:
    draw(name)

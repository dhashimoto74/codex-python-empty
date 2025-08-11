# -*- coding: utf-8 -*-
"""
rt_plot_wdo_mm_fast.py  •  Windows + Python 3.12
Plot RT (quase tempo real) do WDO com as bandas do MM.

Definições (NUNCA usar preço absoluto do MM):
  t0           = 1º snapshot do MM do dia ("âncora")
  WDO@t0       = preço do WDO no instante t0
  centro(t)    = WDO@t0 + [worst_bid(t) - worst_bid@t0]
  faixa(t)     = centro(t) ± (best_ask(t) - best_bid(t)) / 2

Obs: lê do Access (pyodbc) em loop; roda sem Excel/coletor (offline) ou com coletor ligado (RT).
"""

import os
import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from datetime import datetime, timedelta
import time, warnings

# ================== CONFIG ==================
DB = r"C:\Users\User\OneDrive\0 - Vida GOAT\10 Daytrade\Market Maker\MM_Analise.accdb"

# Access schema
TBL_WDO       = "wdo_trades"
COL_WDO_TIME  = "hora_execucao"          # string 'HH:MM:SS.mmm'
COL_WDO_PX    = "preco"

TBL_MM        = "mm_snapshots"
COL_MM_TIME   = "hora_captura"           # datetime
COL_BEST_BID  = "mm_bid_price"
COL_BEST_ASK  = "mm_ask_price"
COL_WORST_BID = "mm_worst_bid_price"

# Parâmetros de performance/visual
BIN             = "5min"     # candles WDO
REFRESH_SEC     = 3          # intervalo de atualização
WINDOW_MIN      = 180        # janela visível (min). Ex.: 180 = 3h. Use None p/ tudo desde t0
MM_RESAMPLE_SEC = 1          # reamostra MM para 1s p/ reduzir pontos (ffill)
WDO_TOP_N       = 80000      # lê só as últimas N linhas (reduz I/O)
READONLY_CONN   = True       # conexão ODBC somente leitura

# Debug
DEBUG_PRINTS     = False
FORCE_SHIFT_ZERO = False     # True: centro = WDO@t0 (teste de fumaça)

# ================== WARNINGS ==================
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning
)

# ================== HELPERS ==================
def to_num(v):
    """Converte string com vírgula/ponto em float. Retorna NaN para '', '-', None."""
    if v is None: return np.nan
    s = str(v).strip()
    if s in ("", "-", "nan", "NaN", "None"): return np.nan
    # normaliza separadores BR/US
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return np.nan

def parse_hhmmssfff_to_ts(hhmm, session_date):
    """Converte 'HH:MM:SS.mmm' (ou 'HH:MM:SS') em Timestamp do mesmo dia de session_date."""
    if pd.isna(hhmm): return pd.NaT
    s = str(hhmm).strip().replace(",", ".")
    t = pd.to_datetime(s, format="%H:%M:%S.%f", errors="coerce")
    if pd.isna(t):
        t = pd.to_datetime(s, format="%H:%M:%S", errors="coerce")
    if pd.isna(t):
        return pd.NaT
    return pd.to_datetime(f"{session_date} {t.strftime('%H:%M:%S.%f')}")

def value_at_or_before(series: pd.Series, ts: pd.Timestamp):
    """Último valor <= ts (pad). Retorna NaN se não existir."""
    if series.empty: return np.nan
    idx = series.index.get_indexer([ts], method="pad")
    if idx[0] == -1: return np.nan
    return series.iloc[idx[0]]

# ================== STATE ==================
anchor_center = None   # WDO@t0
anchor_t0     = None   # 1º snapshot MM do dia
anchor_date   = None

# ================== SETUP ==================
conn_str = rf"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DB}"
if READONLY_CONN:
    conn_str += ";ReadOnly=1;"
conn = pyodbc.connect(conn_str)

plt.ion()
fig, ax = plt.subplots(figsize=(13, 7))

def compute_and_draw():
    global anchor_center, anchor_t0, anchor_date

    # ---------- 1) Descobre a data de sessão pelo último snapshot ----------
    row = pd.read_sql(f"SELECT MAX({COL_MM_TIME}) as mx FROM {TBL_MM}", conn)
    if row.empty or pd.isna(row.loc[0, "mx"]):
        ax.clear(); ax.text(0.5, 0.5, "Sem snapshots MM", ha="center", va="center", transform=ax.transAxes)
        plt.pause(0.5); return

    last_ts = pd.to_datetime(row.loc[0, "mx"])
    session_date = last_ts.normalize().date()
    day_start = pd.Timestamp(session_date)
    day_end   = day_start + pd.Timedelta(days=1)

    # ---------- 2) MM do dia (filtrado no SQL) ----------
    mm = pd.read_sql(
        f"""
        SELECT {COL_MM_TIME},{COL_BEST_BID},{COL_BEST_ASK},{COL_WORST_BID}
        FROM {TBL_MM}
        WHERE {COL_MM_TIME} >= ? AND {COL_MM_TIME} < ?
        ORDER BY {COL_MM_TIME}
        """,
        conn, params=[day_start, day_end]
    )
    if mm.empty:
        ax.clear(); ax.text(0.5, 0.5, "Sem snapshots MM do dia", ha="center", va="center", transform=ax.transAxes)
        plt.pause(0.5); return

    mm[COL_MM_TIME] = pd.to_datetime(mm[COL_MM_TIME], errors="coerce")
    for c in (COL_BEST_BID, COL_BEST_ASK, COL_WORST_BID):
        mm[c] = mm[c].apply(to_num)
    mm = mm.dropna(subset=[COL_MM_TIME]).set_index(COL_MM_TIME).sort_index()

    # resample p/ reduzir densidade (1s)
    if MM_RESAMPLE_SEC and MM_RESAMPLE_SEC > 0:
        mm = mm.resample(f"{MM_RESAMPLE_SEC}s").last().ffill()

    # t0 e reset de âncora se mudou de data
    if (anchor_t0 is None) or (anchor_date != session_date):
        anchor_t0 = mm.index[0]
        anchor_date = session_date
        anchor_center = None

    # ---------- 3) WDO do dia (TOP N mais recentes, depois reordena crescente) ----------
    wdo = pd.read_sql(
        f"""
        SELECT TOP {WDO_TOP_N} {COL_WDO_TIME},{COL_WDO_PX}
        FROM {TBL_WDO}
        ORDER BY {COL_WDO_TIME} DESC
        """,
        conn
    )
    if not wdo.empty:
        wdo = wdo.iloc[::-1].copy()  # ordem crescente
        wdo["ts"] = wdo[COL_WDO_TIME].apply(lambda s: parse_hhmmssfff_to_ts(s, session_date))
        wdo[COL_WDO_PX] = wdo[COL_WDO_PX].apply(to_num)
        wdo = wdo.dropna(subset=["ts", COL_WDO_PX]).sort_values("ts")
    else:
        wdo = pd.DataFrame(columns=["ts", COL_WDO_PX])

    # Define WDO@t0 (âncora do centro)
    if anchor_center is None:
        w0 = wdo[wdo["ts"] <= anchor_t0]
        if w0.empty:
            w0 = wdo[wdo["ts"] >= anchor_t0]
        if w0.empty:
            ax.clear(); ax.text(0.5, 0.5, "Aguardando WDO para ancorar o centro...", ha="center", va="center", transform=ax.transAxes)
            plt.pause(0.5); return
        anchor_center = float(w0.iloc[-1][COL_WDO_PX])
        if DEBUG_PRINTS:
            print(f"[ÂNCORA] t0={anchor_t0} | WDO@t0={anchor_center:.2f}")

    # ---------- 4) Cálculo centro & banda ----------
    wb = mm[COL_WORST_BID].astype(float).copy()
    wb[(wb <= 0) | (~np.isfinite(wb))] = np.nan
    wb = wb.ffill()
    wb0 = value_at_or_before(wb, anchor_t0)
    if np.isnan(wb0):
        wb0 = wb.dropna().iloc[0] if wb.dropna().size else np.nan

    if FORCE_SHIFT_ZERO:
        shift = pd.Series(0.0, index=wb.index)
    else:
        shift = (wb - wb0).fillna(0.0)

    center = anchor_center + shift
    # garante a âncora exata
    center.loc[anchor_t0] = anchor_center
    center = center.sort_index()

    bb = mm[COL_BEST_BID].astype(float)
    ba = mm[COL_BEST_ASK].astype(float)
    spread = (ba - bb).where((ba > 0) & (bb > 0))
    spread = spread.where(spread > 0).ffill()
    half = spread / 2.0

    low  = center - half
    high = center + half

    # ---------- 5) Candles WDO (5min) ----------
    wplot = pd.DataFrame()
    if not wdo.empty:
        wdo5 = wdo.set_index("ts")[COL_WDO_PX].resample(BIN).ohlc().dropna(how="any")
        wplot = wdo5

    # ---------- 6) Janela temporal ----------
    if WINDOW_MIN is not None:
        tmax = max(center.index.max(), wplot.index.max()) if len(wplot) else center.index.max()
        tmin = max(anchor_t0, tmax - pd.Timedelta(minutes=WINDOW_MIN))
    else:
        tmin = anchor_t0
        tmax = max(center.index.max(), wplot.index.max()) if len(wplot) else center.index.max()

    # recortes
    msel = (center.index >= tmin) & (center.index <= tmax)
    if len(wplot):
        wplot = wplot[(wplot.index >= tmin) & (wplot.index <= tmax)]

    # ---------- 7) Plot ----------
    ax.clear()

    # candles WDO
    if len(wplot):
        step = (wplot.index[1] - wplot.index[0]).total_seconds() if len(wplot) > 1 else 300.0
        width_days = (step / (24 * 3600)) * 0.7
        for t, row in wplot.iterrows():
            o, h, l, c = row["open"], row["high"], row["low"], row["close"]
            if not np.isfinite(o + h + l + c) or min(o, h, l, c) <= 0:
                continue
            x = mdates.date2num(pd.to_datetime(t).to_pydatetime())
            ax.vlines(x, l, h, linewidth=1, color="0.4")
            y0, y1 = sorted([o, c])
            ax.add_patch(Rectangle((x - width_days / 2.0, y0),
                                   width_days, max(y1 - y0, 1e-6),
                                   fill=True, color=(0.6, 0.6, 0.6, 0.35), linewidth=0))

    # linhas do range/centro (timestamps do MM)
    if msel.any():
        cx = mdates.date2num(center.index[msel].to_pydatetime())
        ax.step(cx, low[msel].values,  where="post", linewidth=1.8, label="Range Low (âncora + ΔWB − spread/2)")
        ax.step(cx, high[msel].values, where="post", linewidth=1.8, label="Range High (âncora + ΔWB + spread/2)")
        ax.step(cx, center[msel].values, where="post", linestyle="--", linewidth=1.4, label="Centro (âncora + ΔWB)")

    # métrica rápida
    last_center = float(center[msel].iloc[-1]) if msel.any() else np.nan
    last_half   = float(half[msel].iloc[-1]) if msel.any() else np.nan
    last_close  = float(wplot["close"].iloc[-1]) if len(wplot) else np.nan
    dist        = last_close - last_center if np.isfinite(last_close) and np.isfinite(last_center) else np.nan

    # % candles dentro do range (usando bordas do tempo mais próximo)
    pct_inside = np.nan
    if len(wplot) and msel.any():
        lows, highs = [], []
        for t, _ in wplot.iterrows():
            ilow  = low[low.index <= t]
            ihigh = high[high.index <= t]
            lows.append(ilow.iloc[-1] if len(ilow) else np.nan)
            highs.append(ihigh.iloc[-1] if len(ihigh) else np.nan)
        rng_low  = pd.Series(lows,  index=wplot.index)
        rng_high = pd.Series(highs, index=wplot.index)
        inside = (wplot["high"] <= rng_high) & (wplot["low"] >= rng_low)
        if inside.size:
            pct_inside = 100.0 * inside.sum() / inside.size

    ax.text(0.995, 0.98,
            "\n".join([
                f"WDO@t0 (âncora): {anchor_center:.1f}",
                f"Centro atual: {last_center:.1f}" if np.isfinite(last_center) else "Centro atual: n/a",
                f"Spread/2 atual: {last_half:.1f}" if np.isfinite(last_half) else "Spread/2 atual: n/a",
                f"Dist. WDO→centro: {dist:+.1f}" if np.isfinite(dist) else "Dist. WDO→centro: n/a",
                f"% candles dentro: {pct_inside:.1f}%" if np.isfinite(pct_inside) else "% candles dentro: n/a",
            ]),
            ha="right", va="top", transform=ax.transAxes, fontsize=10,
            bbox=dict(boxstyle="round,pad=0.4", alpha=0.15))

    # marca t0 (âncora)
    ax.axvline(mdates.date2num(pd.to_datetime(anchor_t0).to_pydatetime()),
               linestyle="--", linewidth=1, alpha=0.6, color="0.4")
    ax.text(mdates.date2num(pd.to_datetime(anchor_t0).to_pydatetime()),
            anchor_center, "  t0/MM (âncora=WDO)", va="bottom", fontsize=8, alpha=0.7, color="0.4")

    # eixos
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    fig.autofmt_xdate()
    ax.set_title("WDO 5min + Range do MM • Centro = âncora(WDO@t0) + Δworst_bid • Largura = (best_ask−best_bid)/2")
    ax.set_xlabel("Tempo"); ax.set_ylabel("Preço")
    # legenda é custosa em cada redraw; comente se precisar de mais fps
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.25)

    # zoom Y robusto
    yvals = []
    if len(wplot):
        yvals += list(wplot[["open", "high", "low", "close"]].values.flatten())
    if msel.any():
        yvals += list(low[msel].values) + list(high[msel].values)
    yvals = np.array([v for v in yvals if np.isfinite(v) and v > 0])
    if yvals.size:
        lo, hi = np.quantile(yvals, [0.02, 0.98]); pad = (hi - lo) * 0.12 if hi > lo else 10
        ax.set_ylim(lo - pad, hi + pad)

    # deixa o GUI respirar (Windows)
    plt.pause(0.001)

    if DEBUG_PRINTS and msel.any():
        wb_now = wb.dropna().iloc[-1] if len(wb.dropna()) else np.nan
        cur_shift = float(center[msel].iloc[-1] - anchor_center) if np.isfinite(last_center) else np.nan
        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"t0={anchor_t0.time()} | WDO@t0={anchor_center:.2f} | "
              f"wb@t0={wb0:.2f} | wb@now={wb_now:.2f} | "
              f"shift={cur_shift:+.2f} | center={last_center:.2f} | half={last_half:.2f}")

# ================== LOOP ==================
try:
    while True:
        try:
            compute_and_draw()
        except Exception as e:
            # erro “soft” no ciclo: loga e segue
            print("⚠️ loop error:", repr(e))
            time.sleep(1.0)
        time.sleep(REFRESH_SEC)
except KeyboardInterrupt:
    print("⏹️ Encerrado pelo usuário.")
finally:
    try:
        conn.close()
    except:
        pass

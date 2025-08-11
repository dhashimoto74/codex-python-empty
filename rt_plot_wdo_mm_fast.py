# -*- coding: utf-8 -*-
"""
rt_plot_wdo_mm_fast.py  •  Realtime plot (WDO 5m candles + MM range)
Definições (confirmadas):
- Centro(t) = WDO@t0 + [ worst_bid(t) − worst_bid@t0 ]
- Faixa(t)  = Centro(t) ± (best_ask(t) − best_bid(t)) / 2
Obs.: NUNCA usamos preço absoluto do MM no eixo Y; tudo é relativo ao WDO@t0
"""

import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
from datetime import datetime, timedelta
import warnings, time, logging

# ================== CONFIG ==================
DB = r"C:\Users\User\OneDrive\0 - Vida GOAT\10 Daytrade\Market Maker\MM_Analise.accdb"

# Tabelas/colunas
TBL_MM  = "mm_snapshots"
C_MM = dict(
    id="id",
    t="hora_captura",              # DATETIME (Access)
    best_bid="mm_bid_price",
    best_ask="mm_ask_price",
    worst_bid="mm_worst_bid_price",
)

TBL_WDO = "wdo_trades"
C_WDO = dict(
    id="id",
    mm_id="mm_snapshot_id",        # mapeia em qual snapshot foi colhido o trade
    t="hora_execucao",             # TEXT 'HH:MM:SS.mmm'
    px="preco",
)

# Plot/loop
REFRESH_SEC   = 2                 # update a cada X segundos
BIN           = "5min"            # candles WDO
WINDOW_MIN    = 150               # janela rolante (minutos). None = desde t0
MAX_WB_JUMP   = 30.0              # filtro de pulos espúrios no worst_bid (pts)
SPREAD_MIN    = 0.5               # sanity do spread
SPREAD_MAX    = 50.0              # sanity do spread
WDO_MAX_ROWS  = 100000            # limite na carga inicial (protege memória)
MM_RESAMPLE   = "1s"              # grade uniforme p/ MM (melhor step + asof)

# ================== LOG & WARNINGS ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("rt_plot")

# silencia o aviso do pandas sobre SQLAlchemy quando usamos pyodbc
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable.*")
# silencia deprecations específicas (usamos .ffill() em vez de method='ffill')
warnings.filterwarnings("ignore", category=FutureWarning, message=".*is deprecated.*")

# ================== HELPERS ==================
def smart_to_float_series(s: pd.Series) -> pd.Series:
    """Converte strings BR/US para float sem multiplicar por 10 acidentalmente.
    Estratégia:
      - protege o separador decimal (último '.' ou ',')
      - remove demais separadores (milhar)
      - restaura separador como ponto e converte
    """
    s = s.astype(str).str.strip()
    s = s.replace({"": np.nan, "-": np.nan, "nan": np.nan, "NaN": np.nan})

    placeholder = "__DEC__"
    s = s.str.replace(r"([.,])(?!.*[.,])", placeholder, regex=True)
    s = s.str.replace(r"[.,]", "", regex=True)
    s = s.str.replace(placeholder, ".", regex=False)

    return pd.to_numeric(s, errors="coerce")

def parse_hhmmssfff_to_ts(hhmm, session_date):
    """Combina HH:MM:SS(.fff) (texto) com a data da sessão -> Timestamp."""
    if pd.isna(hhmm): return pd.NaT
    s = str(hhmm).strip().replace(",", ".")
    t = pd.to_datetime(s, format="%H:%M:%S.%f", errors="coerce")
    if pd.isna(t):
        t = pd.to_datetime(s, format="%H:%M:%S", errors="coerce")
    if pd.isna(t): return pd.NaT
    return pd.to_datetime(f"{session_date} {t.strftime('%H:%M:%S.%f')}")

def value_at_or_before(series: pd.Series, ts: pd.Timestamp):
    """Último valor <= ts (pandas pad)."""
    if series.empty: return np.nan
    idx = series.index.get_indexer([ts], method="pad")
    if idx[0] == -1: return np.nan
    return series.iloc[idx[0]]

def sane_diff(series: pd.Series, max_jump: float) -> pd.Series:
    """Substitui saltos > max_jump por NaN e faz ffill (suaviza glitches)."""
    s = series.copy()
    diff = s.diff().abs()
    mask_glitch = diff > max_jump
    s.loc[mask_glitch] = np.nan
    return s.ffill()

# ================== STATE / CONN ==================
# Read-only + autocommit evita locks no Access
conn = pyodbc.connect(
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DB + ";READONLY=1;",
    autocommit=True
)

mm_cache  = pd.DataFrame()
wdo_cache = pd.DataFrame()
last_mm_id  = None
last_wdo_id = None

anchor_t0     = None       # Timestamp do 1º snapshot MM do dia
anchor_center = None       # WDO@t0
anchor_date   = None       # date
wb0_anchor    = None       # worst_bid@t0
first_mm_id_today = None   # id do primeiro snapshot do dia

# ================== PLOT SETUP ==================
plt.ion()
fig, ax = plt.subplots(figsize=(13, 7))

range_low_line,  = ax.step([], [], where="post", linewidth=1.8, label="Range Low (WDO@t0 + ΔWB − spread/2)")
range_high_line, = ax.step([], [], where="post", linewidth=1.8, label="Range High (WDO@t0 + ΔWB + spread/2)")
center_line,     = ax.step([], [], where="post", linestyle="--", linewidth=1.4, label="Centro (WDO@t0 + Δ worst bid)")

ann_text = ax.text(0.995, 0.98, "", ha="right", va="top", transform=ax.transAxes, fontsize=10,
                   bbox=dict(boxstyle="round,pad=0.4", alpha=0.15))

# marcador t0 (visibilidade será ligada após 1º load)
t0_line  = ax.axvline(0, linestyle="--", linewidth=1, alpha=0.35, color="0.4")
t0_label = ax.text(0, 0, "  t0/MM (âncora=WDO)", va="bottom", fontsize=8, alpha=0.6, color="0.35")
t0_line.set_visible(False)
t0_label.set_visible(False)

ax.legend(loc="upper left")
ax.grid(True, alpha=0.25)
ax.set_title("Tempo real • WDO 5m + Range do MM\nCentro = WDO@t0 + ΔWorstBid • Largura = BestAsk − BestBid (metade p/ lado)")
ax.set_xlabel("Tempo"); ax.set_ylabel("Preço")
ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
fig.autofmt_xdate()

# ========== Candles ==========
def draw_candles(ax, ohlc: pd.DataFrame):
    """Desenha candles (wicks + corpo) do DataFrame OHLC indexado por ts."""

    # limpa apenas os candles anteriores (mantém lines/labels/etc)
    for artist in list(ax.patches):
        if isinstance(artist, Rectangle): artist.remove()

    if ohlc.empty: return
    idx = ohlc.index

    # largura ~70% do passo
    if len(idx) > 1:
        step = (idx[1] - idx[0]).total_seconds()
    else:
        step = 300.0
    width_days = (step/(24*3600)) * 0.7

    for t, row in ohlc.iterrows():
        o,h,l,c = row["open"],row["high"],row["low"],row["close"]
        if not np.isfinite(o+h+l+c) or min(o,h,l,c) <= 0:
            continue
        x = mdates.date2num(pd.to_datetime(t).to_pydatetime())
        # wick
        ax.vlines(x, l, h, linewidth=1, color="0.35")
        # body
        y0, y1 = sorted([o, c])
        ax.add_patch(Rectangle((x - width_days/2.0, y0),
                               width_days, max(y1-y0, 1e-6),
                               fill=True, color=(0.55,0.65,0.85,0.55), linewidth=0))

# ========== Load inicial ==========
def initial_load_today():
    """Carrega dados do dia (MM/WDO), define âncora t0 e WDO@t0, wb@t0."""
    global mm_cache, wdo_cache, last_mm_id, last_wdo_id
    global anchor_t0, anchor_center, anchor_date, wb0_anchor, first_mm_id_today

    # 1) Determina data da sessão pelo timestamp máximo do MM
    max_t = pd.read_sql(f"SELECT MAX({C_MM['t']}) AS max_t FROM {TBL_MM}", conn)["max_t"].iloc[0]
    if pd.isna(max_t):
        raise RuntimeError("Sem snapshots MM no banco.")
    session_date = pd.to_datetime(max_t).normalize().date()

    day_start = pd.Timestamp.combine(session_date, datetime.min.time())
    day_end   = day_start + timedelta(days=1)

    # 2) MM do dia
    mm_today = pd.read_sql(
        f"SELECT {C_MM['id']},{C_MM['t']},{C_MM['best_bid']},{C_MM['best_ask']},{C_MM['worst_bid']} "
        f"FROM {TBL_MM} "
        f"WHERE {C_MM['t']} >= ? AND {C_MM['t']} < ? "
        f"ORDER BY {C_MM['t']}",
        conn, params=[day_start, day_end]
    )
    if mm_today.empty:
        raise RuntimeError("Sem snapshots MM do DIA.")
    mm_today[C_MM['t']] = pd.to_datetime(mm_today[C_MM['t']], errors="coerce")
    mm_today = mm_today.dropna(subset=[C_MM['t']]).sort_values(C_MM['t'])

    # define t0 e id inicial do dia
    anchor_t0 = pd.to_datetime(mm_today[C_MM['t']].iloc[0])
    anchor_date = session_date
    first_mm_id_today = int(mm_today[C_MM['id']].iloc[0])

    # numéricos MM
    for c in (C_MM['best_bid'], C_MM['best_ask'], C_MM['worst_bid']):
        mm_today[c] = smart_to_float_series(mm_today[c])

    # grade uniforme em 1s
    mm_today = (
        mm_today
        .set_index(C_MM['t']).sort_index()
        .resample(MM_RESAMPLE).ffill()
        .dropna(how='all')
    )
    mm_cache = mm_today.copy()
    last_mm_id = int(mm_today[C_MM['id']].max())

    # 3) WDO do dia (filtrado por horário da sessão e id MM >= first_mm_id_today)
    t_start = day_start.strftime("%H:%M:%S")
    t_end   = (day_end - timedelta(milliseconds=1)).strftime("%H:%M:%S.%f")[:-3]
    wdo = pd.read_sql(
        (
            f"SELECT * FROM ("
            f"SELECT TOP {WDO_MAX_ROWS} {C_WDO['id']},{C_WDO['mm_id']},{C_WDO['t']},{C_WDO['px']} "
            f"FROM {TBL_WDO} "
            f"WHERE {C_WDO['t']} BETWEEN ? AND ? AND {C_WDO['mm_id']} >= ? "
            f"ORDER BY {C_WDO['id']} DESC"
            f") sub ORDER BY {C_WDO['id']}"
        ),
        conn, params=[t_start, t_end, first_mm_id_today]
    )
    if wdo.empty:
        raise RuntimeError("Sem trades WDO ligados ao MM de hoje.")

    wdo[C_WDO['px']] = smart_to_float_series(wdo[C_WDO['px']])
    wdo["ts"] = wdo[C_WDO['t']].apply(lambda s: parse_hhmmssfff_to_ts(s, session_date))
    wdo = wdo.dropna(subset=["ts", C_WDO['px']]).sort_values("ts")
    wdo_cache = wdo.copy()
    last_wdo_id = int(wdo[C_WDO['id']].iloc[-1])

    # 4) Âncora WDO@t0 (trade <= t0; se não houver, primeiro >= t0)
    before = wdo[wdo["ts"] <= anchor_t0]
    after  = wdo[wdo["ts"] >= anchor_t0]
    if not before.empty:
        anchor_center_val = float(before.iloc[-1][C_WDO['px']])
    elif not after.empty:
        anchor_center_val = float(after.iloc[0][C_WDO['px']])
    else:
        raise RuntimeError("Não achei trade do WDO para ancorar o centro.")
    anchor_center = anchor_center_val

    # 5) wb@t0
    wb = mm_today[C_MM['worst_bid']].copy()
    wb = wb.where((wb > 0) & np.isfinite(wb)).ffill()
    wb0 = value_at_or_before(wb, anchor_t0)
    if not np.isfinite(wb0):
        wb0 = wb.dropna().iloc[0] if wb.dropna().size else np.nan
    wb0_anchor = float(wb0) if np.isfinite(wb0) else np.nan

    # liga o marcador t0
    t0x = mdates.date2num(anchor_t0.to_pydatetime())
    t0_line.set_xdata([t0x, t0x])
    t0_label.set_position((t0x, anchor_center))
    t0_line.set_visible(True)
    t0_label.set_visible(True)

    log.info("Âncora definida | t0=%s | WDO@t0=%.2f | wb@t0=%.2f | MM rows=%d | WDO rows=%d",
             anchor_t0.strftime("%H:%M:%S"), anchor_center, wb0_anchor, len(mm_cache), len(wdo_cache))

# ========== Incremental ==========
def fetch_incremental():
    """Busca apenas novos registros de MM e WDO e mantém sessão consistente."""
    global mm_cache, wdo_cache, last_mm_id, last_wdo_id, anchor_date

    # MM novos
    mm_new = pd.read_sql(
        f"SELECT {C_MM['id']},{C_MM['t']},{C_MM['best_bid']},{C_MM['best_ask']},{C_MM['worst_bid']} "
        f"FROM {TBL_MM} WHERE {C_MM['id']} > ? ORDER BY {C_MM['id']}",
        conn, params=[last_mm_id]
    )
    if not mm_new.empty:
        mm_new[C_MM['t']] = pd.to_datetime(mm_new[C_MM['t']], errors="coerce")
        for c in (C_MM['best_bid'], C_MM['best_ask'], C_MM['worst_bid']):
            mm_new[c] = smart_to_float_series(mm_new[c])
        mm_new = (
            mm_new.dropna(subset=[C_MM['t']])
                  .set_index(C_MM['t']).sort_index()
                  .resample(MM_RESAMPLE).ffill()
                  .dropna(how='all')
        )
        mm_cache = pd.concat([mm_cache, mm_new])
        mm_cache = mm_cache[~mm_cache.index.duplicated(keep="last")]
        mm_cache = mm_cache.resample(MM_RESAMPLE).ffill().dropna(how='all')
        last_mm_id = int(mm_cache[C_MM['id']].max())

        # troca de sessão?
        latest_date = mm_cache.index.max().date()
        if anchor_date is not None and latest_date != anchor_date:
            log.info("Mudança de sessão detectada (%s -> %s). Recarregando...", anchor_date, latest_date)
            initial_load_today()
            return

    # WDO novos (filtrados por dia da sessão atual)
    day_start = pd.Timestamp.combine(anchor_date, datetime.min.time())
    day_end   = day_start + timedelta(days=1)
    t_start = day_start.strftime("%H:%M:%S")
    t_end   = (day_end - timedelta(milliseconds=1)).strftime("%H:%M:%S.%f")[:-3]
    wdo_new = pd.read_sql(
        f"SELECT {C_WDO['id']},{C_WDO['mm_id']},{C_WDO['t']},{C_WDO['px']} "
        f"FROM {TBL_WDO} "
        f"WHERE {C_WDO['id']} > ? AND {C_WDO['t']} BETWEEN ? AND ? "
        f"ORDER BY {C_WDO['id']}",
        conn, params=[last_wdo_id, t_start, t_end]
    )
    if not wdo_new.empty:
        wdo_new[C_WDO['px']] = smart_to_float_series(wdo_new[C_WDO['px']])
        wdo_new["ts"] = wdo_new[C_WDO['t']].apply(lambda s: parse_hhmmssfff_to_ts(s, anchor_date))
        wdo_new = wdo_new.dropna(subset=["ts", C_WDO['px']]).sort_values("ts")
        wdo_cache = pd.concat([wdo_cache, wdo_new], ignore_index=True)
        last_wdo_id = int(wdo_cache[C_WDO['id']].max())

        latest_ts = wdo_cache["ts"].max()
        if pd.notna(latest_ts):
            latest_wdo_date = latest_ts.date()
            if anchor_date is not None and latest_wdo_date != anchor_date:
                log.info("Mudança de sessão (WDO) detectada (%s -> %s). Recarregando...", anchor_date, latest_wdo_date)
                initial_load_today()
                return

# ========== Compute + Draw ==========
def compute_and_draw():
    """Recalcula linhas e redesenha dentro da janela rolante."""
    if mm_cache.empty: 
        return

    # saneia worst_bid (Δ) e spread
    wb = mm_cache[C_MM['worst_bid']].where(
        (mm_cache[C_MM['worst_bid']] > 0) & np.isfinite(mm_cache[C_MM['worst_bid']])
    ).ffill()
    wb = sane_diff(wb, MAX_WB_JUMP)

    bb = mm_cache[C_MM['best_bid']].astype(float)
    ba = mm_cache[C_MM['best_ask']].astype(float)
    spread = (ba - bb).where((ba > 0) & (bb > 0))\
                      .where(lambda x: (x >= SPREAD_MIN) & (x <= SPREAD_MAX))\
                      .ffill()

    # anchor wb0
    wb0 = value_at_or_before(wb, anchor_t0)
    if not np.isfinite(wb0):
        wb0 = wb.dropna().iloc[0] if wb.dropna().size else np.nan

    # centro/low/high com base no WDO@t0
    center = pd.Series(anchor_center, index=wb.index) + (wb - wb0).fillna(0.0)
    half   = (spread/2.0).ffill()
    low    = center - half
    high   = center + half

    # janela temporal
    tmax = max(center.index.max(), wdo_cache["ts"].max() if not wdo_cache.empty else center.index.max())
    if WINDOW_MIN is None:
        tmin = anchor_t0
    else:
        tmin = tmax - pd.Timedelta(minutes=WINDOW_MIN)

    # candles WDO (5m) e filtro janela
    wdo5 = pd.DataFrame()
    if not wdo_cache.empty:
        wser = wdo_cache.set_index("ts")[C_WDO['px']].astype(float)
        wdo5 = wser.resample(BIN).ohlc().dropna(how="any")
    wsel = (wdo5.index >= tmin) & (wdo5.index <= tmax)

    # seleção MM na janela
    msel = (center.index >= tmin) & (center.index <= tmax)

    # atualiza step-lines
    if msel.any():
        cx = mdates.date2num(center.index[msel].to_pydatetime())
        range_low_line.set_data(cx,  low[msel].values)
        range_high_line.set_data(cx, high[msel].values)
        center_line.set_data(cx,     center[msel].values)

    # redesenha candles
    if wsel.any():
        draw_candles(ax, wdo5.loc[wsel])

    # métricas
    last_center = float(center[msel].iloc[-1]) if msel.any() else np.nan
    last_half   = float(half[msel].iloc[-1]) if msel.any() else np.nan
    last_close  = float(wdo5.loc[wsel]["close"].iloc[-1]) if wsel.any() else np.nan
    dist        = last_close - last_center if np.isfinite(last_close) and np.isfinite(last_center) else np.nan

    # % candles 5m dentro do range
    pct_inside = np.nan
    if wsel.any() and msel.any():
        rng_low  = low.reindex(wdo5.index, method="pad")
        rng_high = high.reindex(wdo5.index, method="pad")
        chunk = wdo5.loc[wsel]
        inside = (chunk["high"] <= rng_high.loc[wsel]) & (chunk["low"] >= rng_low.loc[wsel])
        if inside.size:
            pct_inside = 100.0 * inside.sum() / inside.size

    ann_text.set_text("\n".join([
        f"Data sessão: {anchor_date}",
        f"WDO@t0 (âncora): {anchor_center:.1f}",
        f"Centro atual: {last_center:.1f}" if np.isfinite(last_center) else "Centro atual: n/a",
        f"Spread/2 atual: {last_half:.1f}" if np.isfinite(last_half) else "Spread/2 atual: n/a",
        f"Dist. WDO→centro: {dist:+.1f}" if np.isfinite(dist) else "Dist. WDO→centro: n/a",
        f"% candles dentro: {pct_inside:.1f}%" if np.isfinite(pct_inside) else "% candles dentro: n/a",
    ]))

    # eixos
    ax.set_xlim(mdates.date2num(tmin.to_pydatetime()), mdates.date2num(tmax.to_pydatetime()))
    yvals = []
    if wsel.any():
        yvals += list(wdo5.loc[wsel][["open","high","low","close"]].values.ravel())
    if msel.any():
        yvals += list(low[msel].values) + list(high[msel].values)
    yvals = np.array([v for v in yvals if np.isfinite(v) and v>0])
    if yvals.size:
        lo, hi = np.quantile(yvals, [0.02, 0.98])
        pad = (hi-lo)*0.12 if hi>lo else 10
        ax.set_ylim(lo-pad, hi+pad)

    # atualiza marcador t0
    t0x = mdates.date2num(anchor_t0.to_pydatetime())
    t0_line.set_xdata([t0x, t0x])
    t0_label.set_position((t0x, anchor_center))

    fig.canvas.draw()
    fig.canvas.flush_events()

# ========== Main loop ==========
def main():
    initial_load_today()
    while True:
        try:
            fetch_incremental()
            compute_and_draw()
            time.sleep(REFRESH_SEC)
        except KeyboardInterrupt:
            print("⏹️ Encerrado pelo usuário.")
            break
        except Exception as e:
            log.warning("Loop error: %r", e)
            time.sleep(1.0)

if __name__ == "__main__":
    try:
        main()
    finally:
        try: conn.close()
        except: pass

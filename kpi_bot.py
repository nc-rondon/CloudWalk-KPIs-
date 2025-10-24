import os
from datetime import date, timedelta, datetime, timezone
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
from connectors.connectors import SessionConnector
from constants import OPENAI_API_KEY, ORGANIZATION_ID

# IA (opcional)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from sqlalchemy import text as _text

# PDF
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

THEME_PRIMARY   = colors.HexColor("#5B6CE1")  # azul
THEME_SUCCESS   = colors.HexColor("#16A34A")  # verde
THEME_DANGER    = colors.HexColor("#DC2626")  # vermelho
THEME_TEXT      = colors.HexColor("#1F2937")  # cinza-900
THEME_MUTED     = colors.HexColor("#6B7280")  # cinza-500
THEME_BG_CARD   = colors.HexColor("#F3F4F6")  # cinza-100

BRA_TZ = timezone(timedelta(hours=-3))  # America/Sao_Paulo (fixo)
WINDOW_DAYS = 28
Z_ALERT = -2.0  # alerta quando zscore < -2
SEGMENT = ["entity", "product", "payment_method"]  # granularidade de alerta
REPORT_DIR = "./reports"


def _fmt_money_br(x):
    return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_int_br(x):
    return f"{int(x):,}".replace(",", ".")

def _today_br() -> date:
    return datetime.now(BRA_TZ).date()


def _ensure_dirs():
    os.makedirs(REPORT_DIR, exist_ok=True)


def _fmt_compact_money_br(x: float) -> str:
    """R$ 243,1 M etc."""
    absx = abs(x)
    if absx >= 1_000_000_000:
        return f"R$ {x/1_000_000_000:,.1f} B".replace(",", "X").replace(".", ",").replace("X", ".")
    if absx >= 1_000_000:
        return f"R$ {x/1_000_000:,.1f} M".replace(",", "X").replace(".", ",").replace("X", ".")
    if absx >= 1_000:
        return f"R$ {x/1_000:,.1f} k".replace(",", "X").replace(".", ",").replace("X", ".")
    return _fmt_money_br(x)

def _auto_font(value_str: str) -> int:
    """Diminui fonte conforme o comprimento do texto."""
    n = len(value_str)
    if n <= 8:   return 20
    if n <= 10:  return 18
    if n <= 12:  return 16
    if n <= 16:  return 14
    return 12



def kpis_for_day(df: pd.DataFrame, d: date) -> dict:
    day = df[df["date"] == d]
    if day.empty:
        return {"date": d, "tpv": 0.0, "tx": 0, "avg_ticket": 0.0}
    tpv = day["amount_transacted"].sum()
    tx = day["quantity_transactions"].sum()
    avg_ticket = float(tpv) / float(tx) if tx else 0.0
    return {"date": d, "tpv": float(tpv), "tx": int(tx), "avg_ticket": float(avg_ticket)}


def growth(a: float, b: float) -> tuple[float, float]:
    delta = a - b
    pct = (delta / b * 100.0) if b and b != 0 else None
    return delta, (round(pct, 2) if pct is not None else None)


def comparable_dates(target: date) -> dict:
    return {
        "d_1": target - timedelta(days=1),
        "w_1": target - timedelta(days=7),
        "m_1": target - timedelta(days=30),
    }



def get_last_available_date(engine, strict_positive: bool = True) -> date | None:
    """
    Retorna o MAX(date) da base (opcionalmente apenas com TPV/Tx > 0).
    """
    where_positive = "AND amount_transacted > 0 AND quantity_transactions > 0" if strict_positive else ""
    sql = _text(f"""
        SELECT MAX(date) AS max_date
        FROM bi.kpi_daily
        WHERE 1=1 {where_positive}
    """)
    with engine.connect() as con:
        row = con.execute(sql).mappings().first()
    return row["max_date"] if row and row["max_date"] else None


def load_data(engine, start: date, end: date, strict_positive: bool = True) -> pd.DataFrame:
    where_positive = "AND amount_transacted > 0 AND quantity_transactions > 0" if strict_positive else ""
    sql = text(f"""
        SELECT
          date,
          entity,
          product,
          price_tier,
          anticipation_method,
          payment_method,
          installments,
          amount_transacted,
          quantity_transactions,
          quantity_of_merchants
        FROM bi.kpi_daily
        WHERE date BETWEEN :start AND :end
          {where_positive}
    """)
    with engine.connect() as con:
        df = pd.read_sql(sql, con, params={"start": start, "end": end}, parse_dates=["date"])

    # normalização
    if not df.empty:
        df["date"] = df["date"].dt.date
        df["amount_transacted"]   = pd.to_numeric(df["amount_transacted"], errors="coerce").fillna(0.0)
        df["quantity_transactions"] = pd.to_numeric(df["quantity_transactions"], errors="coerce").fillna(0).astype(int)
        df["quantity_of_merchants"] = pd.to_numeric(df["quantity_of_merchants"], errors="coerce").fillna(0).astype(int)
        df["installments"] = pd.to_numeric(df["installments"], errors="coerce").fillna(0).astype(int)

    print(f"[load_data] período={start}..{end} rows={len(df)} (strict_positive={strict_positive})")
    return df

def segment_alerts(df: pd.DataFrame, target: date) -> pd.DataFrame:
    grp_cols = ["date"] + SEGMENT
    daily = (
        df.groupby(grp_cols, as_index=False)[["amount_transacted", "quantity_transactions"]]
        .sum()
        .rename(columns={"amount_transacted": "tpv", "quantity_transactions": "tx"})
    )
    daily["avg_ticket"] = daily.apply(lambda r: (r["tpv"] / r["tx"]) if r["tx"] else 0.0, axis=1)
    hist = daily[(daily["date"] < target) & (daily["date"] >= (target - timedelta(days=WINDOW_DAYS)))]
    today = daily[daily["date"] == target].copy()
    if today.empty:
        return pd.DataFrame(columns=SEGMENT + ["metric", "value", "ma", "sd", "zscore"])

    def _z(dfh: pd.DataFrame, metric: str):
        agg = dfh.groupby(SEGMENT)[metric].agg(["mean", "std"]).reset_index()
        base = today[SEGMENT + [metric]].merge(agg, on=SEGMENT, how="left")
        base["zscore"] = (base[metric] - base["mean"]) / base["std"].replace({0: pd.NA})
        base["metric"] = metric
        base = base.rename(columns={metric: "value", "mean": "ma", "std": "sd"})
        return base[SEGMENT + ["metric", "value", "ma", "sd", "zscore"]]

    z_tpv = _z(hist, "tpv")
    z_avg = _z(hist.assign(avg_ticket=hist["tpv"] / hist["tx"].replace({0: pd.NA})), "avg_ticket")

    alerts = pd.concat([z_tpv, z_avg], ignore_index=True)
    alerts = alerts[(alerts["zscore"].notna()) & (alerts["zscore"] < Z_ALERT)]
    alerts = alerts.sort_values("zscore").reset_index(drop=True)
    return alerts


def _fmt_br_number(x, is_pct=False):
    if x is None:
        return "n/a"
    if is_pct:
        return f"{x:.2f}%"
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_summary(today_kpi, comp_kpis) -> str:
    d = today_kpi
    dd = comp_kpis
    lines = []
    lines.append(f"📊 **Resumo diário — {d['date']}**")
    lines.append(f"- TPV: R$ {_fmt_br_number(d['tpv'])} | Tx: {d['tx']:,} | Avg Ticket: R$ {_fmt_br_number(d['avg_ticket'])}")
    lines.append("")
    lines.append("📈 **Comparações**")
    lines.append(f"- vs D-1: Δ R$ {_fmt_br_number(dd['dod_delta'])} ({_fmt_br_number(dd['dod_pct'], True)})")
    lines.append(f"- vs W-1: Δ R$ {_fmt_br_number(dd['wow_delta'])} ({_fmt_br_number(dd['wow_pct'], True)})")
    lines.append(f"- vs M-1: Δ R$ {_fmt_br_number(dd['mom_delta'])} ({_fmt_br_number(dd['mom_pct'], True)})")
    return "\n".join(lines)


def format_alerts(alerts: pd.DataFrame, limit=5) -> str:
    if alerts.empty:
        return "✅ Sem alertas: nenhum segmento abaixo da banda histórica."
    head = alerts.head(limit)
    lines = ["⛳ **Alertas (abaixo da banda histórica −2σ)**"]
    for _, r in head.iterrows():
        seg = " | ".join(str(r[c]) for c in SEGMENT)
        metric = "TPV" if r["metric"] == "tpv" else "Avg Ticket"
        lines.append(
            f"- {seg} → {metric}: valor={r['value']:.2f}, média={r['ma']:.2f}, σ={r['sd']:.2f}, z={r['zscore']:.2f}"
        )
    if len(alerts) > limit:
        lines.append(f"... (+{len(alerts)-limit} alertas)")
    return "\n".join(lines)


def ai_summarize(raw_text: str) -> str:
    load_dotenv()
    api_key = OPENAI_API_KEY
    org = os.getenv("OPENAI_ORG", ORGANIZATION_ID)
    if not api_key or OpenAI is None:
        return raw_text
    client = OpenAI(api_key=api_key, organization=org)
    prompt = [
        {"role": "system", "content": "Você é um assistente de BI. Produza um resumo executivo, claro e objetivo, em PT-BR. Use bullets curtos."},
        {"role": "user", "content": f"Transforme o texto abaixo em um resumo executivo para diretoria:\n\n{raw_text}"},
    ]
    resp = client.responses.create(model="gpt-4o-mini", input=prompt)
    return resp.output_text or raw_text


# ---------- PDF helpers ----------
def _markdown_to_story(md_text: str):
    """Converte um markdown leve em elementos para o PDF."""
    styles = getSampleStyleSheet()
    # Ajustes de estilo
    styles.add(ParagraphStyle(name='TitleMB', parent=styles['Heading1'], fontSize=16, leading=20, spaceAfter=10))
    styles.add(ParagraphStyle(name='BodyMB', parent=styles['BodyText'], fontSize=10.5, leading=14))
    styles.add(ParagraphStyle(name='BulletMB', parent=styles['BodyText'], fontSize=10.5, leading=14, leftIndent=12))
    story = []

    lines = md_text.strip().splitlines()
    title_used = False
    for ln in lines:
        ln = ln.strip()
        if not ln:
            story.append(Spacer(1, 6))
            continue
        # títulos markdown simples
        if ln.startswith("📊 **Resumo diário"):
            story.append(Paragraph(ln.replace("**", ""), styles['TitleMB']))
            title_used = True
            continue
        if ln.startswith("📈 **Comparações"):
            story.append(Paragraph(ln.replace("**", ""), styles['Heading2']))
            continue
        if ln.startswith("⛳ **Alertas"):
            story.append(Spacer(1, 8))
            story.append(Paragraph(ln.replace("**", ""), styles['Heading2']))
            continue
        # bullets
        if ln.startswith("- "):
            story.append(Paragraph("• " + ln[2:], styles['BulletMB']))
        else:
            story.append(Paragraph(ln.replace("**", ""), styles['BodyMB']))

    if not title_used and story:
        story.insert(0, Paragraph("Relatório de KPIs", styles['TitleMB']))
    return story


def on_page(canvas, doc):
    # cabeçalho e rodapé
    canvas.saveState()
    w, h = A4
    canvas.setStrokeColor(THEME_PRIMARY)
    canvas.setFillColor(THEME_PRIMARY)
    canvas.setLineWidth(1)
    canvas.line(36, h-48, w-36, h-48)

    canvas.setFillColor(THEME_TEXT)
    canvas.setFont("Helvetica-Bold", 11)
    canvas.drawString(36, h-40, "Relatório de KPIs • CloudWalk (demo)")
    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(THEME_MUTED)
    canvas.drawRightString(w-36, 20, f"Página {doc.page}")
    canvas.restoreState()

def build_kpi_cards(today_kpi: dict, comp: dict):
    """
    Três cards em 2 linhas cada: título (pequeno, muted) em cima,
    valor grande abaixo (alinhado à direita). Evita truncar e permite quebra.
    """

    def make_card(title: str, value_str: str):
        # linhas do card (1 coluna, 2 linhas)
        title_p = Paragraph(f"<b>{title}</b>", ParagraphStyle(
            "kpiTitle", fontSize=9, textColor=THEME_MUTED, leading=11))
        fs = _auto_font(value_str)
        value_p = Paragraph(
            f'<para alignment="right"><font size="{fs}"><b>{value_str}</b></font></para>',
            ParagraphStyle("kpiValue", fontSize=fs, textColor=THEME_TEXT, leading=fs+2)
        )
        t = Table([[title_p],[value_p]], colWidths=[170])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), THEME_BG_CARD),
            ("BOX", (0,0), (-1,-1), 0.0, colors.white),
            ("LEFTPADDING", (0,0), (-1,-1), 10),
            ("RIGHTPADDING", (0,0), (-1,-1), 10),
            ("TOPPADDING", (0,0), (-1,-1), 6),
            ("BOTTOMPADDING", (0,0), (-1,-1), 8),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
        ]))
        return t

    # valores compactos para caber melhor
    v_tpv = _fmt_compact_money_br(today_kpi["tpv"])
    v_tx  = _fmt_int_br(today_kpi["tx"])
    v_avg = _fmt_compact_money_br(today_kpi["avg_ticket"])

    card_tpv = make_card("TPV", v_tpv)
    card_tx  = make_card("Transações", v_tx)
    card_avg = make_card("Ticket Médio", v_avg)

    # cards lado a lado (3 colunas)
    composite = Table([[card_tpv, card_tx, card_avg]], colWidths=[180,180,180])
    composite.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))

    # ----- comparações (mantém a tabela, mas com fonte um pouco menor) -----
    def arrow_and_color(delta, pct):
        if delta is None or pct is None:
            return ("—", THEME_MUTED)
        if delta >= 0:
            return (f"▲ {_fmt_compact_money_br(delta)} ({pct:.2f}%)", THEME_SUCCESS)
        else:
            return (f"▼ {_fmt_compact_money_br(delta)} ({pct:.2f}%)", THEME_DANGER)

    d1_str, d1_color = arrow_and_color(comp["dod_delta"], comp["dod_pct"])
    w1_str, w1_color = arrow_and_color(comp["wow_delta"], comp["wow_pct"])
    m1_str, m1_color = arrow_and_color(comp["mom_delta"], comp["mom_pct"])

    comp_data = [
        ["vs D-1", d1_str],
        ["vs W-1", w1_str],
        ["vs M-1", m1_str],
    ]
    comp_tbl = Table(comp_data, colWidths=[70, 470])
    comp_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (0,-1), colors.whitesmoke),
        ("TEXTCOLOR", (1,0), (1,0), d1_color),
        ("TEXTCOLOR", (1,1), (1,1), w1_color),
        ("TEXTCOLOR", (1,2), (1,2), m1_color),
        ("BOX", (0,0), (-1,-1), 0.25, colors.HexColor("#E5E7EB")),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.HexColor("#E5E7EB")),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("FONTSIZE", (0,0), (-1,-1), 9),
    ]))

    return composite, comp_tbl


def save_pdf(text: str, pdf_path: str, alerts_df: pd.DataFrame | None,
             today_kpi: dict, comp_dict: dict):
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='TitleMB', parent=styles['Heading1'],
                              fontSize=20, textColor=THEME_TEXT, leading=24, spaceAfter=6))
    styles.add(ParagraphStyle(name='SubMB', parent=styles['BodyText'],
                              fontSize=10.5, textColor=THEME_MUTED, spaceAfter=8))
    styles.add(ParagraphStyle(name='H2MB', parent=styles['Heading2'],
                              fontSize=13, textColor=THEME_TEXT, spaceBefore=10, spaceAfter=6))

    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=64, bottomMargin=36
    )
    story = []

    # Título + data
    story.append(Paragraph("Relatório de KPIs", styles['TitleMB']))
    story.append(Paragraph(f"Resumo Executivo — {today_kpi['date']}", styles['SubMB']))
    story.append(HRFlowable(width="100%", color=THEME_PRIMARY, thickness=1))
    story.append(Spacer(1, 8))

    # Cards e comparações
    cards, comps = build_kpi_cards(today_kpi, comp_dict)
    story.append(cards)
    story.append(Spacer(1, 10))
    story.append(comps)

    # Seção "Insights em texto" (opcional, texto gerado pela IA)
    story.append(Spacer(1, 12))
    story.append(Paragraph("Resumo em Texto", styles['H2MB']))
    # converte bullets simples do markdown em parágrafos
    for ln in text.strip().splitlines():
        ln = ln.strip()
        if not ln:
            story.append(Spacer(1, 2))
            continue
        if ln.startswith("• ") or ln.startswith("- "):
            story.append(Paragraph("• " + ln[2:], styles['BodyText']))
        elif ln.startswith(("📈", "📊", "⛳")):
            story.append(Paragraph(ln, styles['BodyText']))
        else:
            story.append(Paragraph(ln, styles['BodyText']))

    # Alertas
    story.append(Spacer(1, 12))
    story.append(Paragraph("Alertas (abaixo da banda histórica −2σ)", styles['H2MB']))
    if alerts_df is not None and not alerts_df.empty:
        head = alerts_df.head(10).copy()
        head["metric"] = head["metric"].map({"tpv": "TPV", "avg_ticket": "Avg Ticket"})
        data = [["Entidade", "Produto", "Método", "Métrica", "Valor", "Média", "σ", "z"]]
        for _, r in head.iterrows():
            data.append([
                str(r["entity"]), str(r["product"]), str(r["payment_method"]),
                str(r["metric"]), f"{r['value']:.2f}", f"{r['ma']:.2f}",
                f"{r['sd']:.2f}", f"{r['zscore']:.2f}"
            ])
        tbl = Table(data, hAlign="LEFT", colWidths=[60,80,80,60,65,65,35,35])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#EEF2FF")),
            ("TEXTCOLOR", (0,0), (-1,0), THEME_PRIMARY),
            ("GRID", (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,0), (-1,0), 9),
            ("FONTSIZE", (0,1), (-1,-1), 8.5),
            ("ALIGN", (4,1), (-1,-1), "RIGHT"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#FAFAFA")]),
        ]))
        story.append(tbl)
    else:
        story.append(Paragraph("✅ Sem alertas para hoje.", styles['BodyText']))

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)



def run_kpi_bot(target: date | None = None) -> tuple[str, str] | str:
    load_dotenv()
    engine = SessionConnector().session()

    # alvo padrão = ontem em BRT
    requested_target = target or (_today_br() - timedelta(days=1))

    # janela inicial em torno do requested_target
    start = requested_target - timedelta(days=max(60, WINDOW_DAYS + 7))
    end   = requested_target

    df = load_data(engine, start, end, strict_positive=True)

    # Se não houver dados para o dia solicitado, usa o último dia disponível na base
    if df.empty or df[df["date"] == requested_target].empty:
        last_day = get_last_available_date(engine, strict_positive=True)
        if last_day is None:
            print("[run_kpi_bot] ❌ Nenhum dado na base.")
            return "NO_DATA"
        if last_day != requested_target:
            print(f"[run_kpi_bot] ⚠️ Dia {requested_target} sem dados. Usando último dia disponível: {last_day}.")
            # Recarrega janela alinhada ao novo target
            requested_target = last_day
            start = requested_target - timedelta(days=max(60, WINDOW_DAYS + 7))
            end   = requested_target
            df = load_data(engine, start, end, strict_positive=True)

    # KPIs do dia (agora garantido existir)
    target = requested_target
    comps = comparable_dates(target)
    today = kpis_for_day(df, target)
    d_1   = kpis_for_day(df, comps["d_1"])
    w_1   = kpis_for_day(df, comps["w_1"])
    m_1   = kpis_for_day(df, comps["m_1"])

    dod_delta, dod_pct = growth(today["tpv"], d_1["tpv"])
    wow_delta, wow_pct = growth(today["tpv"], w_1["tpv"])
    mom_delta, mom_pct = growth(today["tpv"], m_1["tpv"])

    comp_dict = {
        "dod_delta": dod_delta, "dod_pct": dod_pct,
        "wow_delta": wow_delta, "wow_pct": wow_pct,
        "mom_delta": mom_delta, "mom_pct": mom_pct,
    }

    alerts_df = segment_alerts(df, target)

    summary   = format_summary(today, comp_dict)
    alerts_msg= format_alerts(alerts_df)
    full_text = f"{summary}\n\n{alerts_msg}"
    pretty    = ai_summarize(full_text)

    _ensure_dirs()
    md_path  = os.path.join(REPORT_DIR, f"kpi_report_{target.isoformat()}.md")
    pdf_path = os.path.join(REPORT_DIR, f"kpi_report_{target.isoformat()}.pdf")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(pretty + "\n")

    # >>> usa a versão nova do save_pdf (que monta os cards)
    save_pdf(pretty, pdf_path, alerts_df=alerts_df, today_kpi=today, comp_dict=comp_dict)

    print(pretty)
    print(f"[OK] Salvo:\n- {md_path}\n- {pdf_path}")
    return md_path, pdf_path


if __name__ == "__main__":
    run_kpi_bot()

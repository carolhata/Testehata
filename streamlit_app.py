# streamlit_app.py
# Template ajustado para raspagem de resultados de busca (listings) e exportação Excel
import os
import re
import json
import logging
from io import BytesIO
from urllib.parse import urlparse, urljoin
from typing import List, Literal, Optional, Dict, Any
from datetime import datetime, timezone

import streamlit as st

# MUST be the first Streamlit command in this module
st.set_page_config(
    page_title="Raspador de Listings → Excel",
    page_icon="🧾",
    layout="centered"
)

# safe to import rest
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, PositiveInt, validator

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("listings-scraper")

# =========================
# Secrets (after set_page_config)
# =========================
def get_secret(name: str) -> Optional[str]:
    try:
        return st.secrets.get(name, None)
    except Exception:
        return None

OPENAI_API_KEY = get_secret("OPENAI_API_KEY")
# Tavily optional (leave as is)
TAVILY_API_KEY = get_secret("TAVILY_API_KEY")

# We won't require OPENAI here; keep optional
# =========================
# App settings data model
# =========================
class AppSettings(BaseModel):
    # kept minimal for compatibility with previous app
    scrape_char_limit: PositiveInt = Field(default=8000)

# =========================
# session_state initialization & normalization
# =========================
if "history" not in st.session_state:
    st.session_state.history = []
if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()
# normalize settings to ensure keys
try:
    if not isinstance(st.session_state.settings, dict):
        st.session_state.settings = AppSettings().dict()
    else:
        defaults = AppSettings().dict()
        for k, v in defaults.items():
            if k not in st.session_state.settings:
                st.session_state.settings[k] = v
except Exception:
    st.session_state.settings = AppSettings().dict()

if "scrape_url" not in st.session_state:
    st.session_state.scrape_url = ""
if "scrape_df" not in st.session_state:
    st.session_state.scrape_df = None  # will hold DataFrame of listings

# =========================
# Utilities: parsing helpers
# =========================
def first_text_or_none(el):
    try:
        t = el.get_text(separator=" ", strip=True)
        return t if t else None
    except Exception:
        return None

money_rx = re.compile(r"(R\$[\s\d\.\,kKmM]+|\d[\d\.\,]*\s*R\$)", re.IGNORECASE)
m2_rx = re.compile(r"(\d{1,4}\s*(m²|m2|m²|m\^2))", re.IGNORECASE)
rooms_rx = re.compile(r"(\d+)\s*(quartos|qd|qtos|dormitórios|dormitório)", re.IGNORECASE)
suite_rx = re.compile(r"(\d+)\s*(suítes|suíte|suite)", re.IGNORECASE)
vaga_rx = re.compile(r"(\d+)\s*(vagas|vaga)", re.IGNORECASE)
iptu_rx = re.compile(r"(IPTU[:\s]*R?\$?\s*[\d\.\,kKmM]+)", re.IGNORECASE)
cond_rx = re.compile(r"(Condomínio|Condominio|condomínio|condominio)[:\s]*R?\$?\s*[\d\.\,kKmM]+", re.IGNORECASE)

def is_probably_ad(tag: BeautifulSoup) -> bool:
    """
    Heurística para detectar anúncios/patrocinados: palavras em class/id/text
    """
    text = (tag.get_text(" ", strip=True) or "").lower()
    class_id = " ".join(filter(None, [(" ".join(tag.get("class") or [])).lower() if tag.get("class") else "", (tag.get("id") or "").lower()]))
    # common ad markers
    ad_markers = ["ad", "ads", "patrocinad", "patrocinio", "promoted", "anúncio", "anuncio", "sponsored"]
    for m in ad_markers:
        if m in text or m in class_id:
            return True
    return False

def find_listing_cards(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """
    Tenta localizar os 'cards' de resultados de busca com heurísticas:
    - elementos com classes contendo 'list', 'card', 'result', 'item', 'property', 'listing'
    - evita containers que pareçam anúncios
    """
    candidates = []
    selectors = [
        "[class*='listing']",
        "[class*='card']",
        "[class*='result']",
        "[class*='item']",
        "[class*='property']",
        "[class*='ad']",
        "[class*='anuncio']",
        "li",
        "article",
        "div"
    ]
    # Search for candidate blocks
    for sel in selectors:
        for el in soup.select(sel):
            # ignore tiny elements
            text = (el.get_text(" ", strip=True) or "")
            if len(text) < 30:
                continue
            # skip obvious ads
            if is_probably_ad(el):
                continue
            # ensure it's not a top-level container that contains many results (we want individual cards)
            # heuristics: card should not have many child elements that themselves contain 200+ chars (avoid containers)
            child_texts = [c.get_text(" ", strip=True) for c in el.find_all(recursive=False)]
            if any(len(ct or "") > 400 for ct in child_texts):
                # could be big container; still consider but lower priority
                pass
            candidates.append(el)
    # Deduplicate by id or text snippet
    seen = set()
    filtered = []
    for c in candidates:
        key = (c.get("id") or "") + "|" + (c.get("class")[0] if c.get("class") else "") + "|" + (c.get_text(" ", strip=True)[:200])
        if key in seen:
            continue
        seen.add(key)
        filtered.append(c)
    # Heuristic: many generic divs matched; keep those that have price or square meters or 'quartos'
    final = []
    for c in filtered:
        t = c.get_text(" ", strip=True)
        if money_rx.search(t) or m2_rx.search(t) or rooms_rx.search(t) or "vaga" in t.lower():
            final.append(c)
    # If none found by heuristics, fallback to a small set of filtered items (avoid huge containers)
    if not final:
        # find direct children likely to be list items under a results container
        for container in soup.select("[class*='results'], [id*='results'], [class*='list'], [id*='list']"):
            for li in container.find_all(["li", "article", "div"], recursive=False):
                if is_probably_ad(li):
                    continue
                t = li.get_text(" ", strip=True)
                if len(t) > 50 and (money_rx.search(t) or m2_rx.search(t) or rooms_rx.search(t)):
                    final.append(li)
        # as last resort, try top-level li/article
        if not final:
            for li in soup.find_all(["li", "article"], limit=80):
                if is_probably_ad(li):
                    continue
                t = li.get_text(" ", strip=True)
                if len(t) > 50 and (money_rx.search(t) or m2_rx.search(t) or rooms_rx.search(t)):
                    final.append(li)
    return final

def extract_field_from_card(card, base_url) -> Dict[str, Optional[str]]:
    """
    Extrai heurísticamente os campos requeridos de um elemento 'card'
    """
    text = card.get_text(" ", strip=True) or ""
    # link: prefer the largest anchor in the card
    link = None
    a_tags = card.find_all("a", href=True)
    if a_tags:
        # choose href of the anchor with longest text or first anchor with 'href' that looks internal/external listing
        a_tags_sorted = sorted(a_tags, key=lambda a: len(a.get_text(" ", strip=True) or ""), reverse=True)
        href = a_tags_sorted[0].get("href")
        # make absolute
        if href:
            link = urljoin(base_url, href)
    # endereço: try common address tags or patterns
    address = None
    # try find elements with aria-label or address tag
    addr_el = card.find(["address"]) or card.find(attrs={"aria-label": re.compile(r"endereço|address|localização|localizacao", re.I)})
    if addr_el:
        address = first_text_or_none(addr_el)
    if not address:
        # heuristic: look for text with street markers (Rua, Av., Avenida, Travessa, R., Alameda)
        m = re.search(r"((R\.|Rua|Av\.|Avenida|Travessa|Alameda|Estrada|Rodovia)\s+[^\n,]{3,80})", text, re.I)
        if m:
            address = m.group(1).strip()
    # valor
    valor = None
    m = money_rx.search(text)
    if m:
        valor = m.group(0).strip()
    # condominio
    cond = None
    m = cond_rx.search(text)
    if m:
        cond = m.group(0).split(":", 1)[-1].strip() if ":" in m.group(0) else m.group(0).strip()
    else:
        # try pattern "Condomínio R$ 1.000"
        m2 = re.search(r"(Condom[ií]o[:\s]*R?\$?\s*[\d\.\,kKmM]+)", text, re.I)
        if m2:
            cond = m2.group(1).split(":", 1)[-1].strip()
    # IPTU
    iptu = None
    m = iptu_rx.search(text)
    if m:
        iptu = m.group(0).split(":", 1)[-1].strip() if ":" in m.group(0) else m.group(0).strip()
    # M2
    m2 = None
    m = m2_rx.search(text)
    if m:
        m2 = m.group(1).replace(" ", "")
    # Quartos
    quartos = None
    m = rooms_rx.search(text)
    if m:
        quartos = m.group(1)
    # Suites
    suites = None
    m = suite_rx.search(text)
    if m:
        suites = m.group(1)
    # Vaga(s)
    vagas = None
    m = vaga_rx.search(text)
    if m:
        vagas = m.group(1)
    # fallback: try dedicated spans/labels
    # try to find element labelled "Quartos" etc.
    def find_label_value(card, label_patterns: List[str]) -> Optional[str]:
        for pat in label_patterns:
            el = card.find(string=re.compile(pat, re.I))
            if el:
                # try next sibling or parent text
                parent = el.parent
                # sibling
                sib_text = None
                if parent:
                    # try parent next sibling
                    next_el = parent.find_next_sibling()
                    if next_el:
                        sib_text = first_text_or_none(next_el)
                if not sib_text:
                    # try parent text minus label
                    pt = first_text_or_none(parent)
                    if pt:
                        # remove label word
                        candidate = re.sub(pat, "", pt, flags=re.I).strip()
                        if candidate:
                            sib_text = candidate
                if sib_text:
                    return sib_text
        return None

    if not quartos:
        q = find_label_value(card, ["quartos?", "dormit[oó]rios?", "qtos?", r"(\d)\s*quarto"])
        if q:
            # extract number
            mq = re.search(r"(\d+)", q)
            if mq:
                quartos = mq.group(1)

    if not suites:
        s = find_label_value(card, ["su[ií]tes?", "su[ií]te", "suite"])
        if s:
            ms = re.search(r"(\d+)", s)
            if ms:
                suites = ms.group(1)

    if not vagas:
        v = find_label_value(card, ["vagas?", "vaga"])
        if v:
            mv = re.search(r"(\d+)", v)
            if mv:
                vagas = mv.group(1)

    # final dictionary (normalize empty strings to None)
    return {
        "Endereço": address or None,
        "Valor": valor or None,
        "Condominio": cond or None,
        "IPTU": iptu or None,
        "M2": m2 or None,
        "Quartos": quartos or None,
        "Suites": suites or None,
        "Vaga": vagas or None,
        "Link": link or None
    }

def parse_listings_from_url(url: str, limit: int = 100) -> pd.DataFrame:
    """
    Faz download da página e tenta extrair os listings da página de resultados de busca.
    Retorna DataFrame com as colunas solicitadas.
    """
    logger.info("Fetching URL for listings parse: %s", url)
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ListingsScraper/1.0)",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.exception("HTTP error fetching url")
        raise RuntimeError(f"Falha ao baixar a URL: {e}")

    content_type = resp.headers.get("Content-Type", "")
    if "html" not in content_type.lower():
        raise RuntimeError(f"Conteúdo não-HTML: {content_type}")

    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    cards = find_listing_cards(soup)
    logger.info("Found %d candidate cards", len(cards))
    rows = []
    base_url = resp.url  # after redirects

    count = 0
    for card in cards:
        if count >= limit:
            break
        # double-check not ad
        if is_probably_ad(card):
            continue
        data = extract_field_from_card(card, base_url)
        # require at least Valor or M2 or Link to consider valid listing
        if any([data.get("Valor"), data.get("M2"), data.get("Link")]):
            rows.append(data)
            count += 1

    # deduplicate by Link or (Valor+Endereço)
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get("Link") or "") + "|" + (r.get("Valor") or "") + "|" + (r.get("Endereço") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    df = pd.DataFrame(deduped)
    # normalize columns order
    expected_cols = ["Endereço", "Valor", "Condominio", "IPTU", "M2", "Quartos", "Suites", "Vaga", "Link"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None
    df = df[expected_cols]
    return df

# =========================
# UI: Sidebar controls
# =========================
with st.sidebar:
    st.header("Configuração")
    scrape_mode = st.radio("Tipo de extração", ["Texto completo (fallback)", "Resultados de busca (listings)"], index=1)
    max_listings = st.number_input("Máx de listings a extrair", min_value=5, max_value=500, value=80, step=5)
    st.markdown("---")
    # reset button for debug
    if st.button("Resetar estado (debug)"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.experimental_rerun()

# =========================
# Main UI
# =========================
st.title("🧾 Raspador de resultados de busca → Excel")
st.markdown("Cole a URL da página de resultados de busca (ex.: página com múltiplos anúncios/listings). O scrapper tentará **extrair apenas os resultados de busca** (ignorando anúncios/patrocinados).")

col1, col2 = st.columns([4, 1])
with col1:
    url_input = st.text_input("URL da página de busca", value=st.session_state.scrape_url or "", placeholder="https://exemplo.com/busca?q=apartamento")
with col2:
    run_btn = st.button("Extrair", use_container_width=True)

if run_btn and url_input:
    st.session_state.scrape_url = url_input
    with st.spinner("Raspando página e extraindo resultados..."):
        try:
            if scrape_mode == "Resultados de busca (listings)":
                df = parse_listings_from_url(url_input, limit=int(max_listings))
                if df is None or df.empty:
                    st.warning("Nenhum listing identificado automaticamente. Tente alternar para 'Texto completo' ou passe a URL exata da listagem.")
                else:
                    st.success(f"Extraídos {len(df)} resultados (após heurística).")
                    st.session_state.scrape_df = df
                    # show preview
                    st.dataframe(df.head(50))
            else:
                # fallback to previous behavior: scrape full text (lighter)
                from bs4 import BeautifulSoup as BS
                resp = requests.get(url_input, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
                soup = BS(resp.text, "lxml")
                for tag in soup(["script", "style", "noscript", "template"]):
                    tag.decompose()
                text = re.sub(r"\n\s*\n+", "\n\n", soup.get_text(separator="\n"))
                st.session_state.scrape_df = None
                st.success("Texto da página extraído (use download abaixo para salvar).")
                with st.expander("Prévia do texto raspado"):
                    st.write(text[:10000])
        except Exception as e:
            logger.exception("Erro na extração")
            st.error(f"Falha ao extrair: {e}")

# download/export area
st.markdown("---")
st.subheader("Exportar para Excel")

def df_to_excel_bytes_for_listings(df: pd.DataFrame, sheet_name: str = "listings") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

if st.session_state.get("scrape_df") is not None:
    df_preview = st.session_state.scrape_df.copy()
    st.markdown("Tabela identificada:")
    st.dataframe(df_preview.head(100))
    fname = f"listings_{datetime.now().strftime('%Y%m%d-%H%M%S')}.xlsx"
    st.download_button(
        label="Baixar Excel (listings)",
        data=df_to_excel_bytes_for_listings(df_preview),
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
else:
    st.info("Nenhum DataFrame de listings disponível. Execute uma extração de 'Resultados de busca (listings)'.")
    st.caption("Modo alternativo: extração de texto completo está disponível mas não gera o Excel de listings automaticamente.")

# =========================
# Diagnóstico rápido
# =========================
with st.expander("Diagnóstico técnico"):
    st.json({
        "scrape_url": st.session_state.get("scrape_url"),
        "has_listings_df": bool(st.session_state.get("scrape_df") is not None),
        "scrape_mode": scrape_mode,
    })

st.caption("Heurísticas: o scraper tenta extrair campos principais dos cards de resultados e ignorar anúncios/patrocinados. Para alta precisão, informar domínio específico para ajuste fino.")



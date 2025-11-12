# streamlit_app.py
import os
import re
import logging
from io import BytesIO
from urllib.parse import urljoin, urlparse
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, PositiveInt, validator

# -------------------------
# Config
# -------------------------
st.set_page_config(page_title="Raspador de Anúncios Imobiliários", page_icon="🏠", layout="wide")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("real-estate-scraper")

# -------------------------
# Secrets (OpenAI optional)
# -------------------------
def get_secret(name: str) -> Optional[str]:
    try:
        return st.secrets.get(name, None)
    except Exception:
        return None

OPENAI_API_KEY = get_secret("OPENAI_API_KEY")
TAVILY_API_KEY = get_secret("TAVILY_API_KEY")

# -------------------------
# App settings model (keeps previous pattern)
# -------------------------
class AppSettings(BaseModel):
    max_listing_links: PositiveInt = Field(default=30)
    timeout_sec: PositiveInt = Field(default=15)

    @validator("max_listing_links")
    def clamp_max_links(cls, v):
        return min(v, 200)

if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()

# -------------------------
# Utils: fetching and parsing
# -------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; StreamlitScraper/1.0)",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

def safe_get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.debug(f"safe_get failed {url}: {e}")
        return None

def extract_jsonld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = tag.string
            if not data:
                continue
            parsed = re.sub(r"^\s+|\s+$", "", data)
            obj = __import__("json").loads(parsed)
            # can be list or dict
            if isinstance(obj, list):
                out.extend(obj)
            else:
                out.append(obj)
        except Exception:
            continue
    return out

# Heuristics to find price, area, etc.
RE_PRICE = re.compile(r"(R\$\s?[\d\.,]+)", re.I)
RE_CONDOM = re.compile(r"(condom[ií]nio[:\s]*R?\$?\s*[\d\.,]+)", re.I)
RE_IPTU = re.compile(r"(iptu[:\s]*R?\$?\s*[\d\.,]+)", re.I)
RE_AREA = re.compile(r"(\d{1,4}(?:[.,]\d{1,2})?)\s*(m²|m2|m²)", re.I)
RE_ROOM = re.compile(r"(\d+)\s*(?:quartos|quarto)\b", re.I)
RE_SUITE = re.compile(r"(\d+)\s*(?:su[ií]tes|su[ií]te|suite)\b", re.I)
RE_VAGA = re.compile(r"(\d+)\s*(?:vagas|vaga)\b", re.I)
RE_ADDRESS_LABEL = re.compile(r"end[eé]re[cç]o[:\s]*([^\n\r]+)", re.I)

def text_first_matching(regex: re.Pattern, text: str) -> Optional[str]:
    m = regex.search(text)
    if m:
        return m.group(1).strip()
    return None

def clean_money(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    # keep like R$ 1.234,56
    return raw.strip()

def parse_listing_page(url: str, timeout: int = 15) -> Dict[str, Any]:
    """
    Fetch a single listing page and attempt to extract the requested fields.
    """
    logger.info(f"Parsing listing: {url}")
    res = {"Link": url, "Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
           "M2": None, "Quartos": None, "Suites": None, "vaga": None, "raw_text": ""}
    resp = safe_get(url, timeout=timeout)
    if resp is None:
        res["raw_text"] = ""
        return res

    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n", strip=True)
    res["raw_text"] = text[:1000]  # small preview for debug

    # 1) Try JSON-LD structured data for address/price/area
    try:
        jsonld = extract_jsonld(soup)
        for obj in jsonld:
            if isinstance(obj, dict):
                # common keys: offers.price, address, areaServed, numberOfRooms, floorSize
                offers = obj.get("offers") or obj.get("mainEntityOfPage") or {}
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                    if price and not res["Valor"]:
                        # price could be numeric
                        res["Valor"] = f"R$ {price}" if isinstance(price, (int, float)) else str(price)
                addr = obj.get("address") or obj.get("location") or (obj.get("mainEntityOfPage") or {}).get("address")
                if addr and not res["Endereço"]:
                    if isinstance(addr, dict):
                        # build address string if structured
                        parts = []
                        for k in ("streetAddress", "addressLocality", "addressRegion", "postalCode"):
                            v = addr.get(k)
                            if v:
                                parts.append(str(v))
                        if parts:
                            res["Endereço"] = ", ".join(parts)
                    elif isinstance(addr, str):
                        res["Endereço"] = addr
                # floorSize or area
                area = obj.get("floorSize") or obj.get("area") or obj.get("floorArea")
                if isinstance(area, dict):
                    v = area.get("value") or area.get("name")
                    if v and not res["M2"]:
                        res["M2"] = str(v)
                elif area and not res["M2"]:
                    res["M2"] = str(area)
                # rooms
                rooms = obj.get("numberOfRooms") or obj.get("numRooms")
                if rooms and not res["Quartos"]:
                    res["Quartos"] = str(rooms)
                suites = obj.get("numberOfBedrooms") or obj.get("numberOfSuites")
                if suites and not res["Suites"]:
                    res["Suites"] = str(suites)
    except Exception:
        logger.debug("JSON-LD parse failed", exc_info=True)

    # 2) Regex fallback on visible text
    if not res["Valor"]:
        p = text_first_matching(RE_PRICE, text)
        res["Valor"] = clean_money(p)
    if not res["Condominio"]:
        c = text_first_matching(RE_CONDOM, text)
        res["Condominio"] = clean_money(c)
    if not res["IPTU"]:
        i = text_first_matching(RE_IPTU, text)
        res["IPTU"] = clean_money(i)
    if not res["M2"]:
        a = text_first_matching(RE_AREA, text)
        res["M2"] = a
    if not res["Quartos"]:
        q = text_first_matching(RE_ROOM, text)
        res["Quartos"] = q
    if not res["Suites"]:
        s = text_first_matching(RE_SUITE, text)
        res["Suites"] = s
    if not res["vaga"]:
        v = text_first_matching(RE_VAGA, text)
        res["vaga"] = v

    # 3) Try to detect address labels near DOM elements
    if not res["Endereço"]:
        # common tags: <address>, or label 'Endereço' in nearby elements
        addr_tag = soup.find("address")
        if addr_tag and addr_tag.get_text(strip=True):
            res["Endereço"] = addr_tag.get_text(" ", strip=True)

    if not res["Endereço"]:
        # look for "Endereço" label in page: lines where 'Endereço' appears
        m = RE_ADDRESS_LABEL.search(text)
        if m:
            res["Endereço"] = m.group(1).strip()

    # 4) If still empty, try to extract from title/meta
    if not res["Endereço"]:
        title = soup.title.string if soup.title else None
        if title:
            # sometimes title contains neighborhood / address
            res["Endereço"] = title.strip()

    # Normalize some fields (strip repeated whitespace)
    for k in ["Endereço", "Valor", "Condominio", "IPTU", "M2", "Quartos", "Suites", "vaga"]:
        if res.get(k) and isinstance(res[k], str):
            res[k] = re.sub(r"\s+", " ", res[k]).strip()

    return res

def gather_listing_links(listings_page: str, max_links: int = 50, timeout: int = 15) -> List[str]:
    """
    From a page of listings, collect candidate links to individual ads.
    Heuristics: anchor hrefs that contain keywords or look like internal listing pages.
    """
    resp = safe_get(listings_page, timeout=timeout)
    if resp is None:
        return []
    base = resp.url
    soup = BeautifulSoup(resp.text, "lxml")
    anchors = soup.find_all("a", href=True)
    links = []
    seen = set()
    keywords = ["imovel", "imóvel", "anuncio", "anúncio", "apartamento", "venda", "aluguel", "detalhes", "property"]
    for a in anchors:
        href = a["href"].strip()
        if href.startswith("#") or href.lower().startswith("javascript"):
            continue
        full = urljoin(base, href)
        parsed = urlparse(full)
        # exclude external domains? allow same domain or common listing patterns
        if parsed.scheme not in ("http", "https"):
            continue
        if full in seen:
            continue
        # Heuristic: if anchor text or href contains a keyword, consider it
        anchor_text = (a.get_text(" ", strip=True) or "").lower()
        lower_href = href.lower()
        if any(k in anchor_text for k in keywords) or any(k in lower_href for k in keywords):
            links.append(full)
            seen.add(full)
        else:
            # also include links that look like detail pages: long path with digits
            if re.search(r"/\d{3,}", parsed.path):
                links.append(full)
                seen.add(full)
        if len(links) >= max_links:
            break
    return links

# -------------------------
# Excel helper
# -------------------------
def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "listings") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

# -------------------------
# UI
# -------------------------
st.title("🏠 Raspador de Anúncios Imobiliários — Extração Estruturada")
st.markdown(
    "Cole abaixo a **URL da página de listagens** (por exemplo: resultado de busca de um site de imóveis). "
    "O app tentará acessar cada anúncio e extrair Endereço, Valor, Condomínio, IPTU, M2, Quartos, Suítes, Vaga e Link."
)

col1, col2 = st.columns([3,1])
with col1:
    listings_url = st.text_input("URL da página de listagens", value=st.session_state.get("last_listings_url",""))
    max_links = st.number_input("Máx de anúncios a seguir", min_value=1, max_value=200, value=int(st.session_state.settings["max_listing_links"]))
    timeout_sec = st.number_input("Timeout (s) por requisição", min_value=5, max_value=60, value=int(st.session_state.settings["timeout_sec"]))
with col2:
    run_btn = st.button("Raspar anúncios (extrair campos)")
    clear_btn = st.button("Limpar resultados")

if clear_btn:
    st.session_state.pop("listings_df", None)
    st.success("Resultados limpos.")

if run_btn:
    if not listings_url:
        st.error("Cole a URL da página de listagens antes de rodar.")
    else:
        st.session_state["last_listings_url"] = listings_url
        with st.spinner("Coletando links de anúncios..."):
            links = gather_listing_links(listings_url, max_links=max_links, timeout=timeout_sec)
        if not links:
            st.warning("Nenhum link de anúncio encontrado com heurística padrão — verifique a URL ou aumente o máx de links.")
        else:
            st.info(f"{len(links)} links de anúncio coletados — iniciando extração (máx {max_links}).")
            results = []
            progress_bar = st.progress(0)
            for i, link in enumerate(links[:max_links], start=1):
                parsed = parse_listing_page(link, timeout=timeout_sec)
                results.append(parsed)
                progress_bar.progress(int(i/len(links) * 100))
            progress_bar.empty()
            if results:
                df = pd.DataFrame(results).drop(columns=["raw_text"], errors="ignore")
                # Reorder columns to the desired order
                cols = ["Endereço","Valor","Condominio","IPTU","M2","Quartos","Suites","vaga","Link"]
                for c in cols:
                    if c not in df.columns:
                        df[c] = None
                df = df[cols]
                st.session_state["listings_df"] = df
                st.success(f"Extração concluída: {len(df)} anúncios.")
            else:
                st.warning("Nenhum dado extraído.")

# Show results if present
if st.session_state.get("listings_df") is not None:
    df_out: pd.DataFrame = st.session_state["listings_df"]
    st.markdown("### Resultados (pré-visualização)")
    st.dataframe(df_out.head(200))

    # Download button
    excel_bytes = df_to_excel_bytes(df_out)
    ts_fname = datetime.now().strftime("%Y%m%d-%H%M%S")
    fname = f"anuncios_raspados_{ts_fname}.xlsx"
    st.download_button("Baixar Excel com anúncios", data=excel_bytes, file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Diagnostics / tips
with st.expander("Diagnóstico e dicas"):
    st.write("Dicas para melhorar extração:")
    st.write("- Alguns sites usam JS pesado ou carregam conteúdo dinamicamente; nesses casos a raspagem HTML direta pode não trazer todos os dados.")
    st.write("- Ajuste palavras-chave em `gather_listing_links` se o seu site tem padrões diferentes nos URLs.")
    st.write("- Se quiser, me diga 1 exemplo de URL (um anúncio) que você quer extrair e eu adapto os seletores para ficar 100% preciso.")
    st.write("")
    st.write("Config atual:")
    st.json({
        "last_listings_url": st.session_state.get("last_listings_url",""),
        "max_listing_links": int(max_links),
        "timeout_sec": int(timeout_sec),
    })

st.caption("Esse scraper usa heurísticas; é normal precisar ajustar para sites específicos — posso adaptar os padrões se você me enviar uma URL de exemplo.")

# streamlit_app.py
import os
import re
import json
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
st.set_page_config(page_title="Raspador Zap Imóveis — API-first", page_icon="🏠", layout="wide")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("zap-api-scraper")

# -------------------------
# Secrets (OpenAI/Tavily optional)
# -------------------------
def get_secret(name: str) -> Optional[str]:
    try:
        return st.secrets.get(name, None)
    except Exception:
        return None

OPENAI_API_KEY = get_secret("OPENAI_API_KEY")
TAVILY_API_KEY = get_secret("TAVILY_API_KEY")

# -------------------------
# Settings model
# -------------------------
class AppSettings(BaseModel):
    max_listing_links: PositiveInt = Field(default=50)
    timeout_sec: PositiveInt = Field(default=20)

    @validator("max_listing_links")
    def clamp_max_links(cls, v):
        return min(v, 500)

if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; StreamlitScraper/1.0)",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    # Zap's glue API may accept requests without special headers, but we'll include a referer when possible.
}

# -------------------------
# Helpers
# -------------------------
def safe_get(url: str, timeout: int = 15, extra_headers: Optional[Dict[str,str]] = None) -> Optional[requests.Response]:
    try:
        headers = HEADERS.copy()
        if extra_headers:
            headers.update(extra_headers)
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.debug(f"safe_get failed {url}: {e}")
        return None

def try_zap_glue_api(listings_page: str, max_items: int = 50, timeout: int = 20) -> List[Dict[str, Any]]:
    """
    Tenta obter os anúncios diretamente da API 'glue-api.zapimoveis.com.br'.
    O endpoint e parâmetros podem variar com o tempo — este método tenta uma chamada comum
    para resultados de listagem de apartamentos para venda.
    Retorna lista de objetos de anúncio (bruto) ou [].
    """
    parsed = urlparse(listings_page)
    domain = parsed.netloc.lower()
    # Só aplicamos esta estratégia para zapimoveis.com.br
    if "zapimoveis.com.br" not in domain:
        return []

    # Construir uma request para o endpoint glue-api (heurística conhecida)
    # Parâmetros comuns: business=SALE (venda), category=APARTMENT
    size = max(20, min(max_items, 200))
    api_url = f"https://glue-api.zapimoveis.com.br/v3/listings?business=SALE&category=APARTMENT&page=1&size={size}"

    # Algumas variações usam query params diferentes; tentamos também sem category
    candidates = [api_url,
                  f"https://glue-api.zapimoveis.com.br/v3/listings?business=SALE&page=1&size={size}"]

    for url in candidates:
        try:
            resp = safe_get(url, timeout=timeout, extra_headers={"Referer": listings_page})
            if not resp:
                continue
            j = resp.json()
            # estrutura esperada: j["content"] ou j["listings"] ou j["data"]
            data = j.get("content") or j.get("listings") or j.get("data") or j
            # normalizar para lista de items
            if isinstance(data, dict) and "listings" in data:
                items = data["listings"]
            elif isinstance(data, dict) and "content" in data:
                items = data["content"]
            elif isinstance(data, list):
                items = data
            elif isinstance(data, dict) and "results" in data:
                items = data["results"]
            else:
                # procurar keys que parecem anúncios (cada valor que é lista de dicts)
                items = []
                for v in data.values() if isinstance(data, dict) else []:
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        items = v
                        break
            # Filtrar itens válidos
            if items and isinstance(items, list):
                logger.info(f"Zap glue API returned {len(items)} items from {url}")
                return items[:max_items]
        except Exception as e:
            logger.debug(f"Zap API attempt failed for {url}: {e}")
            continue
    return []

def extract_listing_from_json(item: Dict[str,Any]) -> Dict[str,Any]:
    """
    Extrai campos do objeto JSON retornado pelo API do Zap (ou similares).
    Mapeia de forma defensiva para os campos desejados:
    Endereço, Valor, Condominio, IPTU, M2, Quartos, Suites, vaga, Link
    """
    out = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
           "M2": None, "Quartos": None, "Suites": None, "vaga": None, "Link": None}
    # Estruturas comuns: item may contain 'address', 'price', 'condominiumFee', 'iptuFee', 'usableArea', 'bedrooms','suites','parkingSpaces','url'
    # Adaptar com muitos possíveis nomes
    def get_any(d, *keys):
        for k in keys:
            v = d.get(k) if isinstance(d, dict) else None
            if v is not None:
                return v
        return None

    # Link
    out["Link"] = get_any(item, "link", "url", "absoluteUrl", "listingUrl")
    # Address: may be nested
    address = get_any(item, "address", "place", "location", "addressLocation")
    if isinstance(address, dict):
        parts = []
        for k in ("street", "streetName", "streetAddress", "fullAddress", "address"):
            v = address.get(k) or address.get(k.lower())
            if v:
                parts.append(str(v))
        # locality
        for k in ("neighborhood", "city", "state", "postalCode"):
            v = address.get(k)
            if v:
                parts.append(str(v))
        if parts:
            out["Endereço"] = ", ".join(parts)
    elif isinstance(address, str):
        out["Endereço"] = address

    # Price
    price = get_any(item, "price", "businessPrice", "priceValue", "value")
    if isinstance(price, dict):
        p = price.get("amount") or price.get("value") or price.get("price")
        if p is not None:
            out["Valor"] = f"R$ {p}"
    elif price is not None:
        # numeric or string
        out["Valor"] = str(price)

    # Condomínio / IPTU
    condo = get_any(item, "condominiumFee", "condominium", "condominium_value", "condominiumPrice")
    if condo:
        out["Condominio"] = str(condo)
    iptu = get_any(item, "iptu", "iptuFee", "iptu_value")
    if iptu:
        out["IPTU"] = str(iptu)

    # Area
    area = get_any(item, "usableArea", "area", "buildingArea", "floorArea", "size")
    if isinstance(area, dict):
        area_val = area.get("value") or area.get("amount")
        if area_val:
            out["M2"] = str(area_val)
    elif area is not None:
        out["M2"] = str(area)

    # Bedrooms / suites / parking
    bed = get_any(item, "bedrooms", "bedroom", "numberBedrooms")
    if bed is not None:
        out["Quartos"] = str(bed)
    suites = get_any(item, "suites", "suitesNumber", "numberOfSuites")
    if suites is not None:
        out["Suites"] = str(suites)
    park = get_any(item, "parkingSpaces", "parking", "parkingSpots", "parkingSlots")
    if park is not None:
        out["vaga"] = str(park)

    # fallback: try to find within nested 'characteristics' or 'details' list
    chars = get_any(item, "characteristics", "features", "amenities", "details")
    if isinstance(chars, list):
        # search patterns like "2 quartos", "1 vaga", "64 m²"
        text = " ".join([str(c) for c in chars])
        if not out["Quartos"]:
            m = re.search(r"(\d+)\s+quartos?", text, re.I)
            if m:
                out["Quartos"] = m.group(1)
        if not out["Suites"]:
            m = re.search(r"(\d+)\s+su[ií]tes?", text, re.I)
            if m:
                out["Suites"] = m.group(1)
        if not out["vaga"]:
            m = re.search(r"(\d+)\s+vagas?", text, re.I)
            if m:
                out["vaga"] = m.group(1)
        if not out["M2"]:
            m = re.search(r"(\d{2,4}(?:[.,]\d{1,2})?)\s*(m2|m²)", text, re.I)
            if m:
                out["M2"] = m.group(1)

    # final tidy: strip and normalize
    for k,v in out.items():
        if isinstance(v, str):
            out[k] = re.sub(r"\s+", " ", v).strip()
    return out

# Fallback: generic link gathering (if API not available)
def gather_listing_links_generic(listings_page: str, max_links: int = 50, timeout: int = 20) -> List[str]:
    resp = safe_get(listings_page, timeout=timeout)
    if not resp:
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
        if parsed.scheme not in ("http", "https"):
            continue
        if full in seen:
            continue
        anchor_text = (a.get_text(" ", strip=True) or "").lower()
        lower_href = href.lower()
        if any(k in anchor_text for k in keywords) or any(k in lower_href for k in keywords):
            links.append(full)
            seen.add(full)
        else:
            if re.search(r"/\d{3,}", parsed.path):
                links.append(full)
                seen.add(full)
        if len(links) >= max_links:
            break
    return links

# Excel writer
def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "listings") -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

# -------------------------
# UI
# -------------------------
st.title("🏠 Zap Imóveis — Extração via API (recomendada)")
st.markdown(
    "Cole a URL da página de listagens do Zap Imóveis (ex.: resultados de busca). "
    "O app tentará primeiro usar a API pública (glue-api) para extrair anúncios; "
    "se não for possível, usará heurística de links e raspagem direta."
)

col1, col2 = st.columns([3,1])
with col1:
    listings_url = st.text_input("URL da página de listagens (Zap Imóveis)", value=st.session_state.get("last_listings_url",""))
    max_links = st.number_input("Máx de anúncios a seguir", min_value=5, max_value=500, value=int(st.session_state.settings["max_listing_links"]))
    timeout_sec = st.number_input("Timeout (s) por requisição", min_value=5, max_value=60, value=int(st.session_state.settings["timeout_sec"]))
with col2:
    run_btn = st.button("Extrair anúncios (API-primeiro)")
    clear_btn = st.button("Limpar resultados")

if clear_btn:
    st.session_state.pop("listings_df", None)
    st.success("Resultados limpos.")

if run_btn:
    if not listings_url:
        st.error("Cole a URL da página de listagens antes de rodar.")
    else:
        st.session_state["last_listings_url"] = listings_url
        with st.spinner("Tentando API do Zap..."):
            items = try_zap_glue_api(listings_url, max_items=max_links, timeout=timeout_sec)
        results = []
        if items:
            st.info(f"API do Zap retornou {len(items)} itens — extraindo campos.")
            progress = st.progress(0)
            for i, it in enumerate(items[:max_links], start=1):
                try:
                    parsed = extract_listing_from_json(it if isinstance(it, dict) else dict(it))
                except Exception as e:
                    logger.exception("Erro extraindo item JSON")
                    parsed = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
                              "M2": None, "Quartos": None, "Suites": None, "vaga": None, "Link": None}
                results.append(parsed)
                progress.progress(int(i/len(items) * 100))
            progress.empty()
        else:
            # fallback to generic scraping
            st.warning("API do Zap não retornou itens — tentando heurística de links e raspagem HTML.")
            with st.spinner("Coletando links de anúncios (heurística)..."):
                links = gather_listing_links_generic(listings_url, max_links=max_links, timeout=timeout_sec)
            if not links:
                st.error("Nenhum link de anúncio encontrado com heurística — o site provavelmente carrega via JS. "
                         "Se quiser, podemos usar Playwright (renderização) ou você pode fornecer um exemplo de anúncio para adaptar seletores.")
            else:
                st.info(f"{len(links)} links coletados — iniciando extração de páginas individuais.")
                progress = st.progress(0)
                for i, link in enumerate(links[:max_links], start=1):
                    # reuse the robust 'parse_listing_page' extraction heuristics from prior version (lightweight)
                    parsed = {"Endereço": None, "Valor": None, "Condominio": None, "IPTU": None,
                              "M2": None, "Quartos": None, "Suites": None, "vaga": None, "Link": link}
                    resp = safe_get(link, timeout=timeout_sec)
                    if resp:
                        try:
                            soup = BeautifulSoup(resp.text, "lxml")
                            text = soup.get_text(separator="\n", strip=True)
                            # basic regex extraction
                            price_match = re.search(r"(R\$\s?[\d\.,]+)", text)
                            if price_match:
                                parsed["Valor"] = price_match.group(1)
                            area_match = re.search(r"(\d{2,4}(?:[.,]\d{1,2})?)\s*(m2|m²)", text, re.I)
                            if area_match:
                                parsed["M2"] = area_match.group(1)
                            q_match = re.search(r"(\d+)\s+quartos?", text, re.I)
                            if q_match:
                                parsed["Quartos"] = q_match.group(1)
                            s_match = re.search(r"(\d+)\s+su[ií]tes?", text, re.I)
                            if s_match:
                                parsed["Suites"] = s_match.group(1)
                            v_match = re.search(r"(\d+)\s+vagas?", text, re.I)
                            if v_match:
                                parsed["vaga"] = v_match.group(1)
                            # address
                            addr_tag = soup.find("address")
                            if addr_tag:
                                parsed["Endereço"] = addr_tag.get_text(" ", strip=True)
                            else:
                                # search label
                                m = re.search(r"Endere[cç]o[:\s]*([^\n\r]+)", text, re.I)
                                if m:
                                    parsed["Endereço"] = m.group(1).strip()
                        except Exception:
                            logger.exception("Erro parsing individual page")
                    results.append(parsed)
                    progress.progress(int(i/len(links) * 100))
                progress.empty()

        # Prepare DataFrame and export
        if results:
            df = pd.DataFrame(results)
            # ensure columns in requested order
            cols = ["Endereço","Valor","Condominio","IPTU","M2","Quartos","Suites","vaga","Link"]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            st.session_state["listings_df"] = df
            st.success(f"Extração concluída: {len(df)} anúncios.")
        else:
            st.warning("Nenhum anúncio extraído.")

# Show results if present
if st.session_state.get("listings_df") is not None:
    df_out: pd.DataFrame = st.session_state["listings_df"]
    st.markdown("### Resultados (pré-visualização)")
    st.dataframe(df_out.head(200))

    # Download button
    excel_bytes = df_to_excel_bytes(df_out)
    ts_fname = datetime.now().strftime("%Y%m%d-%H%M%S")
    fname = f"anuncios_zap_{ts_fname}.xlsx"
    st.download_button("Baixar Excel com anúncios", data=excel_bytes, file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Diagnostics / tips
with st.expander("Diagnóstico e dicas"):
    st.write("Dicas para melhorar extração:")
    st.write("- A API do Zap (glue-api) é o caminho ideal: rápido e estruturado. Caso a API mude, adaptaremos os parâmetros.")
    st.write("- Se o site bloquear o acesso, considere usar token/headers adequados ou Playwright para renderizar o JS.")
    st.write("- Se quiser precisão absoluta para um portal específico, envie 1 URL de anúncio e eu adapto os seletores.")
    st.write("")
    st.write("Config atual:")
    st.json({
        "last_listings_url": st.session_state.get("last_listings_url",""),
        "max_listing_links": int(max_links) if 'max_links' in locals() else st.session_state.settings["max_listing_links"],
        "timeout_sec": int(timeout_sec) if 'timeout_sec' in locals() else st.session_state.settings["timeout_sec"],
    })

st.caption("Este aplicativo tenta API-first (Zap glue-api) e decai para heurísticas de scraping quando necessário. Se quiser que seja 100% robusto para um portal, me envie uma URL de exemplo que eu adapto.") 

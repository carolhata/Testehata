# streamlit_app.py
# QuintoAndar scraper — JSON-first optimized + stricter filtering for true listings
import re
import json
import logging
import time
from io import BytesIO
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qs, urlencode

import streamlit as st

# set_page_config MUST be first Streamlit command
st.set_page_config(page_title="QuintoAndar → Listings → Excel (filtered)", page_icon="🏘️", layout="centered")

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel, Field, PositiveInt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("qa-listings-scraper-filtered")

# -------------------------
# App settings & session
# -------------------------
class AppSettings(BaseModel):
    max_pages: PositiveInt = Field(default=5)
    per_request_delay_sec: float = Field(default=0.6)
    open_individual_count: PositiveInt = Field(default=5)

if "settings" not in st.session_state:
    st.session_state.settings = AppSettings().dict()
else:
    defaults = AppSettings().dict()
    for k, v in defaults.items():
        if k not in st.session_state.settings:
            st.session_state.settings[k] = v

if "scrape_url" not in st.session_state:
    st.session_state.scrape_url = ""
if "scrape_df" not in st.session_state:
    st.session_state.scrape_df = None
if "last_raw_sample" not in st.session_state:
    st.session_state.last_raw_sample = None
if "json_keys_sample" not in st.session_state:
    st.session_state.json_keys_sample = None
if "diagnostic_counts" not in st.session_state:
    st.session_state.diagnostic_counts = {}

# -------------------------
# Normalizers / helpers
# -------------------------
money_rx = re.compile(r"[Rr]\$?\s*[\d\.\,kKmM\s]+")
m2_rx = re.compile(r"(\d{1,4})\s*(?:m²|m2|m\^2)?", re.IGNORECASE)
imovel_id_rx = re.compile(r"/imovel/(\d+)", re.IGNORECASE)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_money_to_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(round(float(value)))
    s = str(value)
    s = s.replace("R$", "").replace("r$", "").strip()
    s = re.sub(r"[^\d\,\.]", "", s)
    if not s:
        return None
    # unify
    if "." in s and "," in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "").replace(",", ".")
    try:
        return int(round(float(s)))
    except:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None

def parse_m2_to_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value)
    m = m2_rx.search(s)
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    digits = re.findall(r"\d{1,4}", s)
    return int(digits[0]) if digits else None

def parse_int(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = re.search(r"(\d+)", str(value))
    return int(s.group(1)) if s else None

# -------------------------
# Pagination helper
# -------------------------
def build_page_url(base_url: str, page: int) -> str:
    try:
        parts = list(urlsplit(base_url))
        qs = parse_qs(parts[3])
        if "page" in qs or "pagina" in qs:
            qs["page"] = [str(page)]
            parts[3] = urlencode(qs, doseq=True)
            return urlunsplit(parts)
        if parts[3]:
            parts[3] = parts[3] + "&page=" + str(page)
        else:
            parts[3] = "page=" + str(page)
        return urlunsplit(parts)
    except Exception:
        if "?" in base_url:
            return base_url + f"&page={page}"
        return base_url + f"?page={page}"

# -------------------------
# Robust JSON extraction (unchanged)
# -------------------------
def try_json_load(s: str):
    s = s.strip()
    if not s or (not (s.startswith("{") or s.startswith("["))):
        return None
    try:
        return json.loads(s)
    except Exception:
        start = None
        for i, ch in enumerate(s):
            if ch in "{[":
                start = i
                break
        if start is None:
            return None
        for end in range(len(s)-1, start, -1):
            if s[end] in "}]":
                snippet = s[start:end+1].strip().rstrip(";,")
                try:
                    return json.loads(snippet)
                except Exception:
                    continue
    return None

def extract_json_around_keyword(text: str, keyword: str, window: int = 200000) -> Optional[Dict[str,Any]]:
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return None
    start_search = max(0, idx - 2000)
    pre = text[start_search: idx]
    brace_pos = pre.rfind("{")
    if brace_pos == -1:
        forward = text[idx: idx + 2000]
        brace_pos2 = forward.find("{")
        if brace_pos2 == -1:
            return None
        start = idx + brace_pos2
    else:
        start = start_search + brace_pos
    depth = 0
    end = None
    for i in range(start, min(len(text), start + window)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        return None
    candidate = text[start:end+1].strip().rstrip(";,")
    try:
        return json.loads(candidate)
    except Exception:
        bpos = text.rfind("[", 0, idx)
        if bpos != -1:
            depth = 0
            end2 = None
            for i in range(bpos, min(len(text), bpos + window)):
                if text[i] == "[":
                    depth += 1
                elif text[i] == "]":
                    depth -= 1
                    if depth == 0:
                        end2 = i
                        break
            if end2:
                try:
                    arr = text[bpos:end2+1]
                    return json.loads(arr)
                except Exception:
                    return None
        return None

def extract_json_from_page(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    # 1) script id __NEXT_DATA__
    script = soup.find("script", id="__NEXT_DATA__")
    if script and (script.string or script.get_text()):
        payload = script.string if script.string else script.get_text(" ", strip=True)
        parsed = try_json_load(payload)
        if parsed:
            st.session_state.last_raw_sample = {"probe":"__NEXT_DATA__","len":len(payload)}
            return parsed
    # 2) window assignments
    for s in soup.find_all("script"):
        text = s.string or s.get_text(" ", strip=True) or ""
        if "window.__NEXT_DATA__" in text or "window.__INITIAL_STATE__" in text or "window.__PRELOADED_STATE__" in text:
            m = re.search(r"(window\.__NEXT_DATA__\s*=\s*|window\.__INITIAL_STATE__\s*=\s*|window\.__PRELOADED_STATE__\s*=\s*)(?P<json>[\s\S]+)", text)
            if m:
                right = m.group("json").strip()
                right = re.sub(r";\s*$", "", right)
                parsed = try_json_load(right)
                if parsed:
                    st.session_state.last_raw_sample = {"probe":"window_assignment","len":len(right)}
                    return parsed
                for kw in ("searchResults","listings","properties","pageProps","props","pricing","pricingInfo"):
                    obj = extract_json_around_keyword(right, kw)
                    if obj:
                        st.session_state.last_raw_sample = {"probe":f"rhs_kw_{kw}","len":len(str(obj))}
                        return obj
    # 3) large scripts with relevant keywords
    scripts = [ (s.string or s.get_text(" ", strip=True) or "") for s in soup.find_all("script") ]
    scripts = [s for s in scripts if len(s) > 200 and any(k in s.lower() for k in ("searchresults","listings","properties","pageprops","props","pricing","pricinginfo","pricingInfos"))]
    scripts = sorted(set(scripts), key=lambda x: len(x), reverse=True)
    for txt in scripts:
        parsed = try_json_load(txt)
        if parsed:
            st.session_state.last_raw_sample = {"probe":"script_direct_parse","len":len(txt)}
            return parsed
        for kw in ("searchResults","listings","properties","pageProps","props","pricing","pricingInfo","pricingInfos"):
            obj = extract_json_around_keyword(txt, kw)
            if obj:
                st.session_state.last_raw_sample = {"probe":f"script_kw_{kw}","len":len(str(obj))}
                return obj
    # 4) global search
    for kw in ("searchResults","listings","properties","pageProps","props","pricing","pricingInfo","pricingInfos"):
        obj = extract_json_around_keyword(html, kw)
        if obj:
            st.session_state.last_raw_sample = {"probe":f"global_kw_{kw}","len":len(str(obj))}
            return obj
    st.session_state.last_raw_sample = {"probe":"none","len":min(2000,len(html)),"head":html[:2000]}
    return None

# -------------------------
# STRONGER heuristics: identify probable listing dict
# -------------------------
def is_probable_listing(d: Dict[str, Any], base_url: str = "") -> bool:
    """
    Regras para aceitar um dict como listing:
    - precisa conter price-like OR listedPrice OR pricingInfo
    - e precisa conter id-like key OR slug/url that looks like an individual listing (/imovel/<id>) 
    - rejeita objects where 'path' or 'slug' looks like category (e.g. '/imovel/apartamento', endswith non-numeric)
    """
    if not isinstance(d, dict):
        return False
    keys_lower = {k.lower() for k in d.keys()}
    has_price = any(k in keys_lower for k in ("price","valor","listedprice","saleprice","pricinginfo","pricinginfos","priceLabel"))
    if not has_price:
        # allow if has area and bedrooms but no price? typically listing has price
        maybe_area = any(k in keys_lower for k in ("area","usablearea","size"))
        if not maybe_area:
            return False
    # id-like
    id_keys = [k for k in d.keys() if k.lower() in ("id","listingid","houseid","propertyid","uuid")]
    if id_keys:
        return True
    # check link/slug/path/permalink
    for possible in ("link","url","slug","path","permalink","relativeUrl"):
        if possible in d and d[possible]:
            try:
                val = str(d[possible])
            except:
                continue
            # direct pattern /imovel/<digits>
            if imovel_id_rx.search(val):
                return True
            # sometimes slug is like 'apartamento-2-quartos-895077000' -> endswith digits
            m = re.search(r"(\d{5,})$", val)
            if m:
                return True
            # exclude category slugs which are short words like 'apartamento','piscina'
            # if slug contains '/imovel/' but ends with a word not a number, ignore
            if "/imovel/" in val.lower():
                # if contains digits anywhere -> ok
                if re.search(r"\d{4,}", val):
                    return True
                # else likely a category -> reject
                return False
    # sometimes nested structure contains 'house' or 'listing' string
    combined = json.dumps(d).lower()[:1000]
    if ("house" in combined or "listing" in combined or "property" in combined) and has_price:
        return True
    return False

# -------------------------
# Find listing dicts robustly in JSON (uses is_probable_listing)
# -------------------------
def find_listing_objects(obj: Any, base_url: str = "") -> List[Dict[str, Any]]:
    found = []
    if isinstance(obj, dict):
        # strong candidate
        if is_probable_listing(obj, base_url):
            found.append(obj)
        # check lists under likely keys quickly
        for k, v in obj.items():
            # if v is a list and likely contains listings, check its items
            if isinstance(v, list) and v:
                # quick check of first item keys
                first = v[0]
                if isinstance(first, dict):
                    sample_keys = " ".join(first.keys()).lower()
                    if any(kword in sample_keys for kword in ("price","listedprice","address","slug","id","pricing","bedroom")):
                        for it in v:
                            if isinstance(it, dict) and is_probable_listing(it, base_url):
                                found.append(it)
            # recurse
            found += find_listing_objects(v, base_url)
    elif isinstance(obj, list):
        for item in obj:
            found += find_listing_objects(item, base_url)
    return found

# -------------------------
# Map JSON listing -> normalized row
# -------------------------
def map_listing_json_to_row(d: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    # link detection
    link = None
    for candidate in ("link","url","permalink","slug","path","relativeUrl"):
        if candidate in d and d[candidate]:
            link = d[candidate]
            break
    if link:
        try:
            link = str(link)
            if not link.startswith("http"):
                link = urljoin(base_url, link)
        except:
            link = None
    # ensure we don't keep category URLs
    if link and "/imovel/" not in link and not re.search(r"/imovel/\d+", link):
        # if link ends with common taxonomy words -> drop it
        if re.search(r"/imovel/(apartamento|casa|kitnet|piscina|lavanderia|academia|playground|sauna|elevador|vagas?)", link, re.I):
            link = None

    # address
    address = None; rua=None; bairro=None; cidade=None
    if "address" in d and d.get("address"):
        address = d.get("address")
        if isinstance(address, dict):
            rua = address.get("street") or address.get("logradouro") or address.get("streetAddress") or address.get("formatted")
            bairro = address.get("neighborhood") or address.get("bairro")
            cidade = address.get("city") or address.get("cidade")
        elif isinstance(address, str):
            addr_text = address
            parts = [p.strip() for p in re.split(r"[,\-–]", addr_text) if p.strip()]
            if len(parts) >= 3:
                rua = parts[0]; bairro = parts[1]; cidade = parts[-1]
            elif len(parts) == 2:
                rua = parts[0]; bairro = parts[1]
            else:
                rua = addr_text

    # price
    valor = None
    for k in ("price","valor","listedPrice","salePrice","displayPrice","priceLabel"):
        if k in d and d[k]:
            valor = d[k]; break
    # condo and iptu
    cond = None; iptu = None
    for k in ("condo","condominium","condominiumFee","condoFee"):
        if k in d and d[k]:
            cond = d[k]; break
    for k in ("iptu","propertyTax","tax","iptuValue"):
        if k in d and d[k]:
            iptu = d[k]; break
    # area
    m2 = None
    for k in ("area","usableArea","size","area_m2","usable_area"):
        if k in d and d[k]:
            m2 = d[k]; break
    # quartos suites vagas
    quartos = None; suites = None; vagas = None
    for k in ("bedrooms","bedroomCount","quartos"):
        if k in d and d[k] is not None:
            quartos = d[k]; break
    for k in ("suites","suiteCount","bathrooms"):
        if k in d and d[k] is not None:
            suites = d[k]; break
    for k in ("parkingSpaces","garage","vagas","parking"):
        if k in d and d[k] is not None:
            vagas = d[k]; break
    # ano de construcao
    ano = None
    for k in ("yearBuilt","constructionYear","builtYear","anoConstrucao"):
        if k in d and d[k]:
            try:
                ano = int(d[k]); break
            except:
                pass

    return {
        "Endereço": address or None,
        "Rua": rua,
        "Bairro": bairro,
        "Cidade": cidade,
        "Valor_raw": str(valor) if valor is not None else None,
        "Valor": parse_money_to_int(valor),
        "Condominio_raw": str(cond) if cond is not None else None,
        "Condominio": parse_money_to_int(cond),
        "IPTU_raw": str(iptu) if iptu is not None else None,
        "IPTU": parse_money_to_int(iptu),
        "M2": parse_m2_to_int(m2),
        "Quartos": parse_int(quartos),
        "Suites": parse_int(suites),
        "Vaga": parse_int(vagas),
        "Link": link,
        "bairro": bairro,
        "data_coleta": now_utc_iso(),
        "ano_de_construcao": ano
    }

# -------------------------
# HTML fallback parser (unchanged)
# -------------------------
def parse_cards_with_bs(html: str, base_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards = []
    candidates = soup.select("[data-testid*='property-card'], [class*='listing'], [class*='card'], [class*='result'], li, article")
    seen = set()
    for c in candidates:
        text = c.get_text(" ", strip=True)
        if len(text) < 40:
            continue
        key = text[:200]
        if key in seen:
            continue
        seen.add(key)
        if re.search(r"bairros próximos|valor médio|imóveis para comprar|valor médio", text, re.I):
            continue
        if re.search(r"patrocinad|anúncio|promovido|sponsored|publicidade", text, re.I):
            continue
        a = c.find("a", href=True)
        link = urljoin(base_url, a.get("href")) if a else None
        mval = re.search(r"R\$\s?[\d\.\,kKmM]+", text)
        valor_raw = mval.group(0) if mval else None
        mcond = re.search(r"Condom[ií]nio[:\s]*R?\$?[^\s\,;]+", text, re.I)
        cond_raw = None
        if mcond:
            cond_raw = re.sub(r"Condom[ií]nio[:\s]*", "", mcond.group(0), flags=re.I).strip()
        miptu = re.search(r"IPTU[:\s]*R?\$?[^\s\,;]+", text, re.I)
        iptu_raw = None
        if miptu:
            iptu_raw = re.sub(r"IPTU[:\s]*", "", miptu.group(0), flags=re.I).strip()
        m2 = None
        m = re.search(r"(\d{1,4})\s*(m²|m2|m\^2)", text, re.I)
        if m:
            m2 = m.group(1)
        quartos = None
        m = re.search(r"(\d+)\s*(quartos|dormit[oó]rios|qtos?)", text, re.I)
        if m:
            quartos = m.group(1)
        suites = None
        m = re.search(r"(\d+)\s*(su[ií]tes?|su[ií]te)", text, re.I)
        if m:
            suites = m.group(1)
        vagas = None
        m = re.search(r"(\d+)\s*(vagas?|vaga)", text, re.I)
        if m:
            vagas = m.group(1)
        address = None
        m = re.search(r"(Rua|Av\.|Avenida|Travessa|Alameda|R\.)\s+[^\n,]{5,80}", text)
        if m:
            address = m.group(0).strip()
        cards.append({
            "Endereço": address,
            "Rua": address,
            "Bairro": None,
            "Cidade": None,
            "Valor_raw": valor_raw,
            "Valor": parse_money_to_int(valor_raw),
            "Condominio_raw": cond_raw,
            "Condominio": parse_money_to_int(cond_raw),
            "IPTU_raw": iptu_raw,
            "IPTU": parse_money_to_int(iptu_raw),
            "M2": parse_m2_to_int(m2),
            "Quartos": parse_int(quartos),
            "Suites": parse_int(suites),
            "Vaga": parse_int(vagas),
            "Link": link,
            "bairro": None,
            "data_coleta": now_utc_iso(),
            "ano_de_construcao": None
        })
    return cards

# -------------------------
# Detailed listing-page parser (unchanged logic)
# -------------------------
def parse_listing_page(url: str) -> Dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; QuintoAndarDetailScraper/1.0)", "Accept-Language": "pt-BR,pt;q=0.9"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.exception("failed fetch listing page %s", url)
        return {}
    html = resp.text
    js = extract_json_from_page(html)
    if js:
        candidates = find_listing_objects(js, base_url=resp.url)
        best = None
        for c in candidates:
            if any(k in c for k in ("link","slug","url")) or any(k in c for k in ("price","listedPrice","pricingInfo","pricingInfos")):
                best = c
                break
        if not best and candidates:
            best = candidates[0]
        if best:
            mapped = map_listing_json_to_row(best, resp.url)
            return mapped
    # fallback HTML scraping for details:
    soup = BeautifulSoup(html, "lxml")
    cond = None
    m = re.search(r"Condom[ií]nio[:\s]*R?\$?[\s\d\.\,kKmM]+", html, re.I)
    if m:
        cond = re.sub(r"Condom[ií]nio[:\s]*", "", m.group(0), flags=re.I).strip()
    iptu = None
    m = re.search(r"IPTU[:\s]*R?\$?[\s\d\.\,kKmM]+", html, re.I)
    if m:
        iptu = re.sub(r"IPTU[:\s]*", "", m.group(0), flags=re.I).strip()
    m2 = None
    m = re.search(r"(\d{1,4})\s*(m²|m2|m\^2)", html, re.I)
    if m:
        m2 = m.group(1)
    quartos = None; suites=None; vagas=None
    m = re.search(r'(\d+)\s*(quartos|dormit[oó]rios|qtos?)', html, re.I)
    if m:
        quartos = m.group(1)
    m = re.search(r'(\d+)\s*(su[ií]tes?|su[ií]te)', html, re.I)
    if m:
        suites = m.group(1)
    m = re.search(r'(\d+)\s*(vagas?|vaga)', html, re.I)
    if m:
        vagas = m.group(1)
    address_full = None
    sel = soup.select_one("address") or soup.select_one("[data-testid='address']") or soup.select_one(".address") or soup.select_one(".ListingAddress")
    if sel:
        address_full = sel.get_text(" ", strip=True)
    if not address_full:
        meta = soup.find("meta", {"property":"og:street-address"}) or soup.find("meta", {"name":"og:street-address"})
        if meta and meta.get("content"):
            address_full = meta.get("content")
    rua=None; bairro=None; cidade=None
    if address_full:
        parts = [p.strip() for p in re.split(r"[,\-–]", address_full) if p.strip()]
        if len(parts) >= 3:
            rua = parts[0]; bairro = parts[1]; cidade = parts[-1]
        elif len(parts) == 2:
            rua = parts[0]; bairro = parts[1]
        else:
            rua = address_full
    ano = None
    m = re.search(r"(Ano de constru[cç][ãa]o|Constru[cç][ãa]o[:\s]*)(\d{4})", html, re.I)
    if m:
        ano = int(m.group(2))
    return {
        "Endereço": address_full,
        "Rua": rua,
        "Bairro": bairro,
        "Cidade": cidade,
        "Valor_raw": None,
        "Valor": None,
        "Condominio_raw": cond,
        "Condominio": parse_money_to_int(cond),
        "IPTU_raw": iptu,
        "IPTU": parse_money_to_int(iptu),
        "M2": parse_m2_to_int(m2),
        "Quartos": parse_int(quartos),
        "Suites": parse_int(suites),
        "Vaga": parse_int(vagas),
        "Link": resp.url,
        "bairro": bairro,
        "data_coleta": now_utc_iso(),
        "ano_de_construcao": ano
    }

# -------------------------
# Parse search page (uses improved find_listing_objects)
# -------------------------
def parse_search_page(url: str) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; QuintoAndarScraper/1.0)", "Accept-Language": "pt-BR,pt;q=0.9"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text
    st.session_state.last_raw_sample = html[:8000]
    json_obj = extract_json_from_page(html)
    rows = []
    base_url = resp.url
    # diagnostics counters
    found_candidates = 0
    accepted = 0
    rejected = 0
    if json_obj:
        # sample keys for diagnostics
        try:
            top_keys = list(json_obj.keys())[:20] if isinstance(json_obj, dict) else None
            st.session_state.json_keys_sample = {"top_keys": top_keys}
        except Exception:
            st.session_state.json_keys_sample = None

        listing_dicts = find_listing_objects(json_obj, base_url=base_url)
        found_candidates = len(listing_dicts)
        for d in listing_dicts:
            row = map_listing_json_to_row(d, base_url)
            # apply final plausibility filter: link must be imovel or Valor/M2 present
            if row.get("Link") and imovel_id_rx.search(str(row.get("Link"))):
                rows.append(row); accepted += 1
            elif row.get("Valor") or row.get("M2"):
                rows.append(row); accepted += 1
            else:
                rejected += 1
        # deeper lists fallback (rare)
        if rows:
            st.session_state.diagnostic_counts = {"candidates": found_candidates, "accepted": accepted, "rejected": rejected}
            return rows
        # traverse for lists deeper
        def traverse_for_lists(o):
            found = []
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, list):
                        found.append(v)
                    else:
                        found += traverse_for_lists(v)
            elif isinstance(o, list):
                for it in o:
                    found += traverse_for_lists(it)
            return found
        lists = traverse_for_lists(json_obj)
        for lst in lists:
            for it in lst:
                if isinstance(it, dict):
                    candidates = find_listing_objects(it, base_url=base_url)
                    for d in candidates:
                        row = map_listing_json_to_row(d, base_url)
                        if row.get("Link") and imovel_id_rx.search(str(row.get("Link"))):
                            rows.append(row); accepted += 1
                        elif row.get("Valor") or row.get("M2"):
                            rows.append(row); accepted += 1
                        else:
                            rejected += 1
        st.session_state.diagnostic_counts = {"candidates": found_candidates, "accepted": accepted, "rejected": rejected}
        if rows:
            return rows
    # fallback: parse cards with bs
    cards = parse_cards_with_bs(html, base_url)
    return cards

# -------------------------
# Orchestrator: multiple pages + dedupe
# -------------------------
def parse_multiple_pages(base_url: str, pages: int) -> pd.DataFrame:
    all_rows = []
    for p in range(1, pages + 1):
        page_url = build_page_url(base_url, p)
        logger.info("Fetching page %s", page_url)
        try:
            items = parse_search_page(page_url)
            for it in items:
                # filter again: keep only rows that look like listings
                if it.get("Link") and imovel_id_rx.search(str(it.get("Link"))):
                    all_rows.append(it)
                elif it.get("Valor") or it.get("M2"):
                    all_rows.append(it)
                else:
                    # skip likely category / taxo links
                    continue
        except Exception as e:
            logger.exception("Error parsing page %s", page_url)
        time.sleep(float(st.session_state.settings.get("per_request_delay_sec", 0.6)))
    if not all_rows:
        return pd.DataFrame()
    # dedupe by Link or Endereço+Valor
    seen = set()
    clean = []
    for r in all_rows:
        key = (r.get("Link") or "") + "|" + str(r.get("Valor") or "") + "|" + str(r.get("Endereço") or "")
        if key in seen:
            continue
        seen.add(key)
        clean.append(r)
    df = pd.DataFrame(clean)
    cols = ["Endereço", "Rua", "Bairro", "Cidade", "Valor_raw", "Valor", "Condominio_raw", "Condominio",
            "IPTU_raw", "IPTU", "M2", "Quartos", "Suites", "Vaga", "Link", "bairro", "data_coleta", "ano_de_construcao"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    return df

# -------------------------
# Enrichment: open individual ads
# -------------------------
def enrich_with_listing_pages(df: pd.DataFrame, max_to_open: int = 5) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    links = [l for l in df["Link"].tolist() if l]
    opened = 0
    for link in links:
        if opened >= max_to_open:
            break
        # prefer only real listing links
        if not imovel_id_rx.search(str(link)):
            continue
        try:
            detail = parse_listing_page(link)
            if not detail:
                continue
            mask = df["Link"] == link
            if not mask.any():
                matches = df[df["Link"].fillna("").str.contains(link.split("?")[0])]
                if not matches.empty:
                    mask = df["Link"].fillna("").str.contains(link.split("?")[0])
            for idx in df[mask].index:
                for k, v in detail.items():
                    if k not in df.columns:
                        continue
                    if (df.at[idx, k] is None) or (pd.isna(df.at[idx, k])) or (str(df.at[idx, k]).strip() == ""):
                        df.at[idx, k] = v
            opened += 1
            time.sleep(float(st.session_state.settings.get("per_request_delay_sec", 0.6)))
        except Exception as e:
            logger.exception("error enriching link %s", link)
            continue
    return df

# -------------------------
# Helpers: sanitize DataFrame for display / download
# -------------------------
import math
import json

def _to_json_str_safe(x):
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        try:
            return str(x)
        except Exception:
            return ""

def sanitize_df_for_display(df: pd.DataFrame, max_cell_len: int = 2000) -> pd.DataFrame:
    """
    Converte colunas que contenham dict/list/objects em strings seguras para PyArrow,
    trunca strings muito longas e garante tipos em colunas numéricas esperadas.
    """
    if df is None or df.empty:
        return df
    df2 = df.copy()

    # Normalize expected numeric columns
    numeric_cols = ["Valor", "Condominio", "IPTU", "M2", "Quartos", "Suites", "Vaga"]
    for c in numeric_cols:
        if c in df2.columns:
            # coerce to numeric, preserve NaN if not convertible
            try:
                df2[c] = pd.to_numeric(df2[c], errors="coerce").astype("Int64")
            except Exception:
                # fallback: attempt to parse strings with parse_money_to_int if available
                try:
                    df2[c] = df2[c].apply(lambda v: parse_money_to_int(v) if not pd.isna(v) else None).astype("Int64")
                except Exception:
                    pass

    # Convert complex types to strings and truncate long strings
    for col in df2.columns:
        # skip already-safe numeric / datetime types
        try:
            if pd.api.types.is_numeric_dtype(df2[col]) or pd.api.types.is_datetime64_any_dtype(df2[col]):
                continue
        except Exception:
            # if dtype inspect fails, continue to cleaning logic
            pass

        # map lists/dicts to json strings and truncate
        def _cell_clean(v):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            # dict/list/tuple -> json string
            if isinstance(v, (dict, list, tuple)):
                s = _to_json_str_safe(v)
            else:
                s = str(v)
            # truncate
            if len(s) > max_cell_len:
                return s[:max_cell_len] + " ...[truncated]"
            return s
        try:
            df2[col] = df2[col].apply(_cell_clean)
        except Exception:
            # last resort: convert whole column to string safely
            try:
                df2[col] = df2[col].astype(str).apply(lambda s: s if len(s) <= max_cell_len else s[:max_cell_len] + " ...[truncated]")
            except Exception:
                # if even that fails, set as empty strings to avoid pyarrow crash
                df2[col] = df2[col].apply(lambda _: None)
    return df2


# -------------------------
# UI
# -------------------------
st.title("🏘️ QuintoAndar — Extrator filtrado → Excel")
st.markdown("Cole a URL da página de busca do QuintoAndar (ex.: Bela Vista). O scraper agora filtra fortemente objetos não-listing (categorias/amenities) e prioriza anúncios reais. Ative 'Abrir anúncios individuais' para enriquecer (teste 5).")

col1, col2 = st.columns([4, 1])
with col1:
    url_input = st.text_input("URL da página de busca (QuintoAndar)", value=st.session_state.scrape_url or "", placeholder="https://www.quintoandar.com.br/comprar/imovel/bela-vista-sao-paulo-sp-brasil...")
with col2:
    run_btn = st.button(f"Extrair ({st.session_state.settings['max_pages']} páginas)", use_container_width=True)

with st.sidebar:
    st.header("Opções")
    st.number_input("Páginas a raspar (padrão 5)", min_value=1, max_value=50, value=int(st.session_state.settings["max_pages"]), key="max_pages_input")
    st.slider("Delay entre requests (segundos)", 0.0, 5.0, float(st.session_state.settings.get("per_request_delay_sec", 0.6)), step=0.1, key="delay_input")
    open_individual = st.checkbox("Abrir anúncios individuais (testar 5)", value=False)
    if st.button("Resetar estado (debug)"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.experimental_rerun()

# sync sidebar inputs
try:
    st.session_state.settings["max_pages"] = int(st.session_state.get("max_pages_input", st.session_state.settings["max_pages"]))
    st.session_state.settings["per_request_delay_sec"] = float(st.session_state.get("delay_input", st.session_state.settings["per_request_delay_sec"]))
except Exception:
    pass

if run_btn and url_input:
    st.session_state.scrape_url = url_input
    pages = int(st.session_state.settings.get("max_pages", 5))
    st.info(f"Iniciando extração de até {pages} páginas (JSON-first, filtro estrito).")
    try:
        df = parse_multiple_pages(url_input, pages)
        if df.empty:
            st.warning("Nenhum listing identificado. Verifique se a URL é realmente uma página de resultados e se a página carrega via XHR. Veja diagnóstico abaixo.")
            if st.session_state.last_raw_sample:
                with st.expander("Amostra do HTML (diagnóstico)"):
                    st.text(st.session_state.last_raw_sample.get("head", st.session_state.last_raw_sample)[:2000])
        else:
            st.success(f"Extraídos {len(df)} registros (após deduplicação).")
            st.session_state.scrape_df = df
            st.dataframe(df.head(80))
            if open_individual:
                st.info(f"Abrindo até {st.session_state.settings.get('open_individual_count',5)} anúncios (detalhes)...")
                df_enriched = enrich_with_listing_pages(df.copy(), max_to_open=int(st.session_state.settings.get("open_individual_count",5)))
                st.session_state.scrape_df = df_enriched
                st.success("Enriquecimento concluído (detalhes dos anúncios).")
                st.dataframe(df_enriched.head(80))
            if pages >= 5:
                if st.button("Raspar mais páginas (próximas 5)"):
                    st.session_state.settings["max_pages"] = pages + 5
                    st.experimental_rerun()
    except Exception as e:
        logger.exception("Falha ao extrair")
        st.error(f"Falha ao extrair: {e}")

st.markdown("---")
st.subheader("Exportar Excel")
def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "listings"):
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.read()

# === [INÍCIO BLOCO: Exibição e Download dos Resultados] ===
if st.session_state.get("scrape_df") is not None:
    df_preview = st.session_state.scrape_df.copy()

    # 🔒 Sanitiza para exibição (remove objetos incompatíveis com PyArrow)
    df_preview_display = sanitize_df_for_display(df_preview, max_cell_len=2000)

    st.markdown("Prévia dos resultados (limpa para exibição):")
    st.dataframe(df_preview_display.head(100))  # mostra até 100 linhas sanitizadas

    # ✅ Gera arquivo Excel seguro para download (serializa dicts/lists em strings)
    fname = f"quintoandar_listings_filtered_{datetime.now().strftime('%Y%m%d-%H%M%S')}.xlsx"

    # Use uma função de export segura (converte dict/list em JSON strings antes de salvar)
    def df_to_excel_bytes_safe(df: pd.DataFrame, sheet_name: str = "listings"):
        df_export = df.copy()
        for col in df_export.columns:
            try:
                if df_export[col].apply(lambda x: isinstance(x, (dict, list, tuple))).any():
                    df_export[col] = df_export[col].apply(lambda v: _to_json_str_safe(v) if v is not None else "")
            except Exception:
                # se falhar ao checar, converte col inteira para string
                try:
                    df_export[col] = df_export[col].astype(str)
                except Exception:
                    df_export[col] = df_export[col].apply(lambda v: str(v) if v is not None else "")
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name=sheet_name)
        bio.seek(0)
        return bio.read()

    st.download_button(
        "Baixar Excel (listings)",
        data=df_to_excel_bytes_safe(df_preview),
        file_name=fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Sem DataFrame disponível. Execute uma extração.")
# === [FIM BLOCO: Exibição e Download dos Resultados] ===


with st.expander("Diagnóstico / Amostras (útil para ajustes)"):
    st.write("URL atual:", st.session_state.get("scrape_url"))
    st.write("Config:", st.session_state.settings)
    st.write("Última amostra (probe/head):")
    st.json(st.session_state.get("last_raw_sample", {}))
    st.write("JSON keys sample (raiz):")
    st.json(st.session_state.get("json_keys_sample", {}))
    st.write("Diagnostic counts (candidates / accepted / rejected):")
    st.json(st.session_state.get("diagnostic_counts", {}))

st.caption("Notas: agora o scraper filtra fortemente objetos não-listing (categorias/amenities). Se ainda aparecerem links incorretos, cole aqui o conteúdo de 'Diagnostic counts' e 5 exemplos de 'Link' do Excel (posso ajustar a regex de detecção /imovel/).")

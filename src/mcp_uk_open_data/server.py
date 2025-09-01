import asyncio, os, requests
from typing import Any, Dict, List
from pydantic import BaseModel, Field, HttpUrl
from mcp.server import Server

LOG_LEVEL = os.getenv("LOG_LEVEL","INFO")
ALLOWED = {"data.gov.uk","ons.gov.uk","statistics.gov.uk"}
srv = Server("uk-open-data")

def _allowed(url:str)->bool:
    from urllib.parse import urlparse
    d = urlparse(url).netloc.split(":")[0].lower()
    return any(d==x or d.endswith("." + x) for x in ALLOWED)

class SearchArgs(BaseModel):
    query: str
    rows: int = Field(5, ge=1, le=50)

class DatasetShowArgs(BaseModel):
    id: str

class FetchJsonArgs(BaseModel):
    url: HttpUrl

@srv.tool()
def ping()->str: return "pong"

@srv.tool()
def search_data_gov_uk(args: SearchArgs)->List[Dict[str,Any]]:
    r = requests.get("https://data.gov.uk/api/3/action/package_search",
                     params={"q":args.query,"rows":args.rows}, timeout=30)
    r.raise_for_status()
    out=[]
    for x in r.json().get("result",{}).get("results",[]):
        out.append({
            "title": x.get("title"),
            "id": x.get("id"),
            "organization": (x.get("organization") or {}).get("title"),
            "notes": (x.get("notes") or "")[:240],
            "resources": [{"format":res.get("format"),"url":res.get("url")} for res in x.get("resources",[])]
        })
    return out

@srv.tool()
def dataset_show(args: DatasetShowArgs)->Dict[str,Any]:
    r = requests.get("https://data.gov.uk/api/3/action/package_show",
                     params={"id":args.id}, timeout=30)
    r.raise_for_status()
    return r.json().get("result",{})

@srv.tool()
def fetch_json(args: FetchJsonArgs)->Dict[str,Any]:
    url = str(args.url)
    if not _allowed(url): raise ValueError("URL not in allow-list")
    r = requests.get(url, timeout=30); r.raise_for_status()
    return r.json()

async def amain(): 
    if LOG_LEVEL in ("INFO","DEBUG"): print("[mcp-uk-open-data] starting stdio...", flush=True)
    await srv.run_stdio()

def main(): asyncio.run(amain())
if __name__ == "__main__": main()

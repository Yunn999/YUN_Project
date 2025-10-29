from googleapiclient.discovery import build 
# 구글 API 클라이언트. build("youtube","v3", developerKey=...)로 YouTube Data API 객체를 만듦

from datetime import datetime, timedelta
import pandas as pd
import pendulum, os, json, re, math, time
# pendulum: 타임존 지원이 좋은 날짜/시간 라이브러리(여기선 KST 변환/표시
from typing import Optional, List

# ── 설정 ─────────────────────────────────────────────────────────────────
API_KEY =  '???'
SAVE_DIR = "./Utube_data/data_collect/collect/"
STATE_PATH = os.path.join(SAVE_DIR, "state.json") 
# state.json: 마지막 실행 시각 저장 → 다음 실행 시 증분 수집 (지난 수집 이후 새로 생긴 데이터만 추가로 가져오는 방식)
MASTER_PATH = os.path.join(SAVE_DIR, "master_latest.parquet") 
# master_latest.parquet: 최신 스냅샷 테이블. videoId 기준으로 최신 1행만 유지.
CATEGORY_MAP = {
    "1":"Film & Animation","2":"Autos & Vehicles","10":"Music","15":"Pets & Animals",
    "17":"Sports","18":"Short Movies","19":"Travel & Events","20":"Gaming",
    "22":"People & Blogs","23":"Comedy","24":"Entertainment","25":"News & Politics",
    "26":"Howto & Style","27":"Education","28":"Science & Technology",
    "29":"Nonprofits & Activism","30":"Movies","31":"Anime/Animation","32":"Action/Adventure",
    "33":"Classics","34":"Comedy","35":"Documentary","36":"Drama","37":"Family",
    "38":"Foreign","39":"Horror","40":"Sci-Fi/Fantasy","41":"Thriller",
    "42":"Shorts","43":"Shows","44":"Trailers"
}
KST = pendulum.timezone("Asia/Seoul")   # 한국시간 pendulum.timezoone("Asia/Seoul")

# ── 유틸 ─────────────────────────────────────────────────────────────────
# Optional[str] => str일수도 있고 아닐수도있고 str이면 str, 아니면 None
def classify_tier(subscriber_count: Optional[int]) -> str:  # " -> str " : 이 함수는 문자열을 반환.
    if subscriber_count is None:
        return "Unknown"
    c = int(subscriber_count)
    if c < 100_000: return "Tier 1"
    if c < 1_000_000: return "Tier 2"
    if c < 10_000_000: return "Tier 3"
    return "Tier 4"

def extract_hashtags(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    import re
    tags = re.findall(r"#\w+", text)
    return ", ".join(tags) if tags else None

# STATE_PATH가 있으면 JSON으로 읽어서 마지막 실행 시각을 가져옴. 없으면 {}
def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# SAVE_DIR가 없으면 만들고, state.json으로 저장.
def save_state(d: dict):
    os.makedirs(SAVE_DIR, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False) # json.dump는 파이썬 객체를 JSON 형식으로 변환해 “파일에 바로 써주는” 함수


# 유튜브가 주는 UTC ISO 시간(예: '2025-08-05T05:51:09Z')을 KST로 바꾼 뒤 'YYYY-MM-DD HH:MM:SS' 형식 문자열로 변환.
def to_kst_str(dt_utc_iso: str) -> str:
    # '2025-08-05T05:51:09Z' → '2025-08-05 14:51:09' (KST)
    dt = pendulum.parse(dt_utc_iso).in_timezone(KST)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# 한국 영상
def _snippet_is_korean(snippet: dict) -> bool:
    """videos.list의 snippet에서 한국어 메타가 보이면 True"""
    if not snippet:
        return False
    lang_keys = ["defaultAudioLanguage", "defaultLanguage"]
    for k in lang_keys:
        v = (snippet.get(k) or "").lower()
        if v.startswith("ko"):
            return True
    return False

# KOREA-ONLY: 텍스트 한글 비율/감지 보조
_HANGUL_RE = re.compile(r'[가-힣]')

def _hangul_ratio(s: str) -> float:
    """문자 중 한글 비율(알파벳/한글 기준)"""
    if not s:
        return 0.0
    letters = [ch for ch in s if ch.isalpha()]
    if not letters:
        return 0.0
    han = sum('가' <= ch <= '힣' for ch in letters)
    return han / len(letters)

def _is_korean_video(snippet: dict, channel_snippet: Optional[dict], ratio_threshold: float = 0.10) -> bool:
    """
    한국 판정 규칙(하나라도 만족하면 True):
      1) snippet.default(Audio)Language 가 ko*
      2) 채널 국가(country)가 KR
      3) 제목/설명/채널명에서 한글 비율이 ratio_threshold 이상 (기본 10%)
    """
    # 1) 영상 언어 메타
    if _snippet_is_korean(snippet):
        return True

    # 2) 채널 국가 메타
    if channel_snippet:
        country = (channel_snippet.get("country") or "").upper()
        if country == "KR":
            return True

    # 3) 텍스트 기반
    title = (snippet.get("title") or "")
    desc  = (snippet.get("description") or "")
    ch    = (snippet.get("channelTitle") or "")
    txt   = f"{title} {desc} {ch}"
    if bool(_HANGUL_RE.search(txt)) and _hangul_ratio(txt) >= ratio_threshold:
        return True

    return False



# ── 핵심: 하루 2000 유닛 이하로 수집 1회 실행 ─────────────────────────────
def run_collect_once(keywords: list[str], 
                     daily_quota_units: int = 2000,
                     published_after_default: str = "2024-07-30T00:00:00Z") -> pd.DataFrame:
    """
    - search.list(100) + videos.list(1) + channels.list(1) ≈ 102 유닛/페이지 기준으로 제한
    - 결과 컬럼: id, videoId, title, description, channelTitle, publishedAt,
                fetchedDate, business_date, viewCount, likeCount, commentCount,
                category, subscriberCount, shortsHashtags, channelTier, url, created_at
    """
    # 저장 폴더 보장. / 유튜브 API 클라이언트 생성.
    os.makedirs(SAVE_DIR, exist_ok=True)    
    yt = build("youtube", "v3", developerKey=API_KEY)

    # 증분 수집 publishedAfter (2시간 버퍼 => “지난 실행 시각에서 2시간을 뒤로 더 당겨서 다시 시작하는 안전 여유 구간”) 
    st = load_state()       # state.json에서 상태를 읽어 딕셔너리로 반환받습니다
    last_iso = st.get("last_run_iso", published_after_default)
    # 상태 딕셔너리 st에서 키 "last_run_iso"를 꺼냅니다. 빈 dict이면 published_after_default
    pa = pendulum.parse(last_iso).in_timezone("UTC") - timedelta(hours=2)
    # last_iso 문자열을 Pendulum으로 파싱하여 DateTime 객체로 만듭니다 / 시각을 UTC 기준으로 변환합니다 
    # 2시간 백오프. 마지막 실행시각보다 2시간 더 이전으로 시작점을 당겨 겹치게 만드는 안전장치입니다.
    published_after = pa.strftime("%Y-%m-%dT%H:%M:%SZ")
    # 위에서 만든 UTC 시각 pa를 RFC3339/ISO8601의 Zulu 표기(끝에 Z)로 포맷합니다

    # 한 페이지(50개) 처리 최소 유닛
    SEARCH_COST, VIDEOS_COST, CHANNELS_COST = 100, 1, 1
    MIN_PAGE_COST = SEARCH_COST + VIDEOS_COST + CHANNELS_COST
    remaining = daily_quota_units

    # 중복 방지: 과거에 수집한 videoId는 다시 안 넣음.
    seen = set()
    if os.path.exists(MASTER_PATH):
        try:
            seen.update(pd.read_parquet(MASTER_PATH)["videoId"].tolist())
        except Exception:
            pass

    rows = []
    fetched_dt = pendulum.now(KST)
    fetched_str = fetched_dt.strftime("%Y-%m-%d %H:%M:%S")
    business_date = fetched_dt.to_date_string()

    for kw in keywords:
        next_token = None
        while True:
            # 키워드별로 페이지네이션 돌면서 예산을 체크
            if remaining < MIN_PAGE_COST:
                # 예산 소진
                break

            # 1) search.list
            # yt.search() => YouTube Data API v3의 search 리소스 핸들러를 얻음.
            # search 리소스의 list 메서드 호출 준비 아직 네트워크 요청은 안 나감. “이런 파라미터로 요청할게요”라고 요청 객체를 구성하는 단계.
            sr = yt.search().list(
                q=kw, type="video", part="snippet", 
                # q 검색 쿼리 문자열 없으면 광범위해져서 api를 소모하고 원하는 결과를 못 얻는다 유튜브이기 때문에 카테고리별로 얻어온다
                # type 동영상만 => 지정하지 않으면 channel이나 playlist도 섞여 들어온다
                # part 검색결과에서 어떤 필드 세트를 담아올지
                # snippet은 **영상의 기본 정보(제목, 설명 일부, 채널, 썸네일, publishTime 등)**를 포함
                # 중요한 점: 카테고리 ID(categoryId)는 여기엔 없음  그래서 뒤에서 videos.list(part="snippet,statistics")로 다시 상세 조회
                
                maxResults=50, publishedAfter=published_after,
                # 페이지당 최대 항목 수. API 상한이 50이라 최대치를 사용.
                # publishedAfter=published_after => 이 시간 이후에 업로드된 결과만 필터.
                
                order="date", regionCode="KR", relevanceLanguage="ko",
                # 정렬을 최신 업로드 순으로 / 대한민국 지역 기준으로 결과를 현지화 / 한국어 선호도 힌트를 제공, 랭킹의 가중치?
                pageToken=next_token
            ).execute() # 여기서 실제 HTTP 요청 전송 -> YouTube API 서버가 응답을 보내고, 파이썬 dict로 결과가 돌아옴 그걸 sr 변수에 담는다
            
            remaining -= SEARCH_COST # 사용한 만큼 api 사용량 감소 

            items = sr.get("items", [])     # 검색 응답 sr에서 결과 리스트를 꺼냅니다. 키가 없을 때를 대비해 기본값 []
            video_ids = []  
            channel_ids = set()

            for it in items:
                vid = it["id"].get("videoId")
                if not vid or vid in seen:  # 중복 skip
                    continue

                video_ids.append(vid)
                channel_ids.add(it["snippet"]["channelId"])

            if not video_ids:
                next_token = sr.get("nextPageToken")
                if not next_token: break
                continue

            # 2) videos.list (최대 50)
            vmap = {}
            rr = yt.videos().list(
                part="snippet,statistics", id=",".join(video_ids)
            ).execute()
            remaining -= VIDEOS_COST
            for v in rr.get("items", []):
                vmap[v["id"]] = v

            # 3) channels.list (최대 50)
            cmap_stat = {}
            cmap_snip = {}
            rr = yt.channels().list(    
                part="statistics,snippet", id=",".join(list(channel_ids)[:50])
            ).execute()
            remaining -= CHANNELS_COST
            for c in rr.get("items", []):
                cid = c["id"]
                cmap_stat[cid] = c.get("statistics", {})
                cmap_snip[cid] = c.get("snippet", {})

            # 4) 결과 행 생성
            for vid in video_ids:
                v = vmap.get(vid)
                if not v: 
                    continue

                sni = v["snippet"]
                ch_snip = cmap_snip.get(sni["channelId"], {})
                # ★ KOREA-ONLY: 한국 판정. 하나라도 만족 못하면 skip
                if not _is_korean_video(sni, ch_snip, ratio_threshold=0.10):
                    continue

                sts = v.get("statistics", {})
                chs = cmap_stat.get(sni["channelId"], {})

                rows.append({
                    "videoId": vid,

                    # sni
                    "title": sni.get("title"),
                    "description": sni.get("description"),
                    "channelTitle": sni.get("channelTitle"),
                    "publishedAt": to_kst_str(sni.get("publishedAt")),
                    "category": CATEGORY_MAP.get(sni.get("categoryId",""), "Unknown"),
                    "shortsHashtags": extract_hashtags(sni.get("description")),

                    "fetchedDate": fetched_str,
                    "business_date": business_date,

                    # sts
                    "viewCount": int(sts.get("viewCount", 0) or 0),
                    "likeCount": int(sts.get("likeCount", 0) or 0),
                    "commentCount": int(sts.get("commentCount", 0) or 0),

                    # chs 
                    "subscriberCount": int(chs.get("subscriberCount", 0) or 0),
                    "channelTier": classify_tier(chs.get("subscriberCount")),

                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "created_at": fetched_str,   # DB에 넣을 때 생성시각
                })
                seen.add(vid)

            next_token = sr.get("nextPageToken")
            if not next_token: break
            time.sleep(0.3)
        
        if remaining < MIN_PAGE_COST:
            break

    # 5) DataFrame + id 부여
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # ----------------------------------------------------------------------------------------------------------------------------
    # 
    start_id = 1
    if os.path.exists(MASTER_PATH):
        try:
            m = pd.read_parquet(MASTER_PATH)    # 마스터 파일을 판다스 데이터프레임으로 불러옴
            if "id" in m.columns and len(m):    # 마스터에 id 컬럼이 있고, 행이 1개 이상인지 확인
                start_id = int(m["id"].max()) + 1   
        except Exception:
            pass
    df.insert(0, "id", range(start_id, start_id + len(df)))

    # 6) 스냅샷/마스터 저장
    snap_name = f"snapshot_{business_date}_{fetched_dt.strftime('%H%M%S')}.parquet" # 최종 예: snapshot_2025-08-14_071530.parquet
    pd.DataFrame(rows).to_parquet(os.path.join(SAVE_DIR, snap_name), index=False)

    try:
        if os.path.exists(MASTER_PATH):
            base = pd.read_parquet(MASTER_PATH)
            merged = pd.concat([base, df], ignore_index=True)
            merged = (merged.sort_values("created_at")                          # 시간 기준(created_at)으로 정렬합니다. 최신이 뒤로 오게함함
                            .drop_duplicates(subset=["videoId"], keep="last"))  # 같은 videoId가 여러 개면 마지막 것(=가장 최신) 만 남기고 중복을 지움움
        else:
            merged = df
        merged.to_parquet(MASTER_PATH, index=False)
    except Exception:
        pass
    
    # ---------------------------------------------------------------------------------------------------------------------------
    save_state({"last_run_iso": pendulum.now("UTC").to_iso8601_string()})
        # .to_iso8601_string()=> 예: "2025-10-27T04:10:23+00:00"
    return df

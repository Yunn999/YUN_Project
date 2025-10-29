# scheduler_collect.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pendulum, signal, sys, time

from yt_collect_budgeted import run_collect_once

import logging, os
from logging.handlers import RotatingFileHandler
# 로그 파일 크기가 일정 용량을 넘으면 자동으로 “파일을 굴려서(rotate)” 새 파일로 이어 쓰고, 오래된 로그는 지정한 개수만큼만 보관하는 핸들러

LOG_DIR = "./Utube_data/data_collect/logs"
os.makedirs(LOG_DIR, exist_ok=True)

fh = RotatingFileHandler(f"{LOG_DIR}/collect.log", maxBytes=2_000_000, backupCount=3)   # backupCount: 보관 백업 개수.

logging.basicConfig(level=logging.INFO, handlers=[fh])
# 로그 임계치(level)를 INFO로 설정 → INFO 이상(INFO·WARNING·ERROR·CRITICAL)만 기록
# 출력 대상(handlers)로 리스트에 넣은 핸들러만 사용합니다. 여기서는 fh(RotatingFileHandler) 하나만 달림
# 즉, 로그가 파일로만 가고, 콘솔로는 출력되지 않음
# 로그는 INFO 이상만 남기고, fh(파일 핸들러: 용량 자동 회전) 로 파일에만 저장


log = logging.getLogger("sched") # 이름표가 sched인 전용 로거를 씀



KEYWORDS = [
    'Film & Animation', 'Autos & Vehicles', 'Music', 'Pets & Animals',
    'Sports', 'Short Movies', 'Travel & Events', 'Gaming', 'People & Blogs',
    'Comedy', 'Entertainment', 'News & Politics', 'Howto & Style',
    'Education', 'Science & Technology', 'Nonprofits & Activism',
    'Movies', 'Anime/Animation', 'Action/Adventure', 'Classics', 'Comedy',
    'Documentary', 'Drama', 'Family', 'Foreign', 'Horror', 'Sci-Fi/Fantasy',
    'Thriller', 'Shorts', 'Shows', 'Trailers'
]

def job():
    try:
        df = run_collect_once(keywords=KEYWORDS ,daily_quota_units=2000)    # 유튜브 데이터를 한 번 수집
        # === 파일명: UtubeDataMMDD-HH.csv (예: UtubeData0814-07.csv) ===
        now = pendulum.now("Asia/Seoul")                                    # 한국시간 기준 현재 시각을 얻음
        fname = f"UtubeData{now.format('MMDD')}-{now.format('HH')}.csv"     # UtubeData1027-13.csv
        out_dir = "./Utube_data/data_collect/collect"                       # 저장경로
        os.makedirs(out_dir, exist_ok=True)                                 # 파일만듦
        out_path = os.path.join(out_dir, fname)                             # 디렉터리와 파일명을 합쳐 전체 경로
        if not df.empty:                                                    
            df.to_csv(out_path, index=False, encoding="utf-8-sig")          # 저장
        log.info(f"DONE rows={len(df)} -> {out_path}")                      # 처리 결과를 정보 로그로 남김
    except Exception as e:
        log.exception(f"job error: {e}")                                    # 에러 로그

if __name__ == "__main__":
    kst = pendulum.timezone("Asia/Seoul")                                   # 한국 시간
    sched = BackgroundScheduler(timezone=kst)                               # 예약 시간 계산을 KST로 일관 처리
    # 매일 07:00, 12:00, 18:00 실행
    trig = CronTrigger(minute="0", hour="07,12,18", timezone=kst)          # 매일 한국시간 07시, 12시, 18시에 동작

    sched.add_job(job, trig, id="yt_collect")      
    # 첫번째 파라미터는 실행할 함수 / 방금 만든 크론 트리거 / 식별자 문자열
    # 스케줄러에 job을 trig 일정으로 실행하는 작업을 등록하고, 관리용 이름을 yt_collect로 붙이는 코드

    def _shutdown(signum=None, frame=None):
        print("\nStopping scheduler…")
        try:
            sched.shutdown(wait=False)
        except Exception:
            pass
        print("Stopped.")
        sys.exit(0)       # 프로세스를 정상 종료 코드 0으로 종료

    signal.signal(signal.SIGINT, _shutdown)     # Ctrl+C 입력(SIGINT)이 오면 _shutdown을 호출하도록 연결
    signal.signal(signal.SIGTERM, _shutdown)    # 시스템이 보내는 일반 종료 신호(SIGTERM)도 _shutdown으로 처리
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _shutdown)


    print("Scheduler started (07:00/12:00/18:00 KST)")
    sched.start()

    try:
        while True:
            time.sleep(1)   # 1초씩 쉬면서 CPU 점유를 최소화
    except KeyboardInterrupt:
        _shutdown()

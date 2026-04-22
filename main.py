import os
import json
import uuid
import datetime
import time
import random
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from deck_parser import scrape_pokecabook_results, fetch_deck_from_official

# .env 読み込み（任意）
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except ImportError:
    pass

# --- 設定 ---
SPREADSHEET_NAME = 'Pokelix DB'
USER_ID = 'system-bot'
MAX_DECKS_PER_RUN = 50

# Supabase（.env に設定されていれば二重書き込み有効）
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_KEY and 'ここに' not in SUPABASE_SERVICE_KEY)

def setup_gspread():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    if not os.path.exists(creds_path):
        print("エラー: credentials.json が見つかりません。")
        return None
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    return client

# --- Supabase REST API ヘルパー ---

def supabase_headers():
    return {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates'  # deck_code重複はスキップ
    }

def supabase_upsert_decks(rows):
    """
    analyzed_decks テーブルに一括 upsert する。
    rows: list of dicts with keys: deck_code, archetype_id, cards_json, event_rank
    deck_code + archetype_id が一致する行は上書きしない（ignore-duplicates）
    """
    if not SUPABASE_ENABLED:
        return
    url = f'{SUPABASE_URL}/rest/v1/analyzed_decks'
    payload = [
        {
            'user_id': USER_ID,
            'deck_code': r['deck_code'],
            'archetype_id': r['archetype_id'],
            'cards_json': r['cards_json'],
            'event_rank': r['event_rank'] if r['event_rank'] != 'ALL' else None,
        }
        for r in rows
    ]
    try:
        res = requests.post(url, headers=supabase_headers(), json=payload, timeout=30)
        if res.status_code in (200, 201):
            print(f"  [Supabase] analyzed_decks に {len(payload)} 件 upsert 完了")
        else:
            print(f"  [Supabase] upsert 失敗 {res.status_code}: {res.text[:200]}")
    except Exception as e:
        print(f"  [Supabase] upsert エラー: {e}")

def supabase_get_existing_codes():
    """analyzed_decks から既存の deck_code 一覧を取得"""
    if not SUPABASE_ENABLED:
        return set()
    url = f'{SUPABASE_URL}/rest/v1/analyzed_decks?select=deck_code'
    existing = set()
    offset = 0
    limit = 1000
    try:
        while True:
            res = requests.get(
                url,
                headers={**supabase_headers(), 'Range': f'{offset}-{offset+limit-1}'},
                timeout=30
            )
            if res.status_code not in (200, 206):
                break
            data = res.json()
            if not data:
                break
            for row in data:
                existing.add(row['deck_code'])
            if len(data) < limit:
                break
            offset += limit
    except Exception as e:
        print(f"  [Supabase] 既存コード取得エラー: {e}")
    return existing

def run_all_scrapers():
    if SUPABASE_ENABLED:
        print("✅ Supabase 二重書き込みモード ON")
    else:
        print("⚠️  Supabase 未設定 → スプレッドシートのみ書き込み")
        print("   .env に SUPABASE_SERVICE_KEY を設定すると注目カード集計が自動更新されます")

    client = setup_gspread()
    if not client: return

    try:
        ss = client.open(SPREADSHEET_NAME)
        config_sheet = ss.worksheet('bot_config')
        all_configs = config_sheet.get_all_values()

        arch_sheet = ss.worksheet('deck_archetypes')
        arch_data = arch_sheet.get_all_values()
        name_to_id = {row[1]: row[0] for row in arch_data[1:] if len(row) >= 2}
    except Exception as e:
        print(f"シートの読み込みに失敗しました: {e}")
        return

    configs_to_run = []
    print(f"--- 全集計プロセス開始 (合計: {len(all_configs)-1} 設定) ---")

    for i, row in enumerate(all_configs[1:]):
        if len(row) < 4: continue
        aname, aid, aurl, active = row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip().upper()
        if active in ['TRUE', 'TURE', '1', 'YES', 'OK', '✅']:
            if not aid or len(aid) < 10:
                aid = name_to_id.get(aname, "")
                if not aid: continue
            configs_to_run.append({"name": aname, "id": aid, "url": aurl})

    # Supabase 既存コードを先にまとめて取得（重複チェック用）
    supabase_existing = supabase_get_existing_codes()
    if SUPABASE_ENABLED:
        print(f"  [Supabase] 既存コード数: {len(supabase_existing)} 件")

    for i, config in enumerate(configs_to_run):
        print(f"\n[{i+1}/{len(configs_to_run)}] {config['name']} の集計を開始...")
        run_single_scraper(ss, config['id'], config['url'], supabase_existing)
        if i < len(configs_to_run) - 1:
            wait = random.uniform(30, 60)
            print(f"アーキタイプ間休憩中 ({wait:.1f}秒)...")
            time.sleep(wait)

    print("\nすべてのアーキタイプの巡回が完了しました！お疲れ様でした。")

def run_single_scraper(ss, archetype_id, url, supabase_existing=None):
    try:
        sheet = ss.worksheet('analyzed_decks')
    except Exception as e:
        print(f"analyzed_decks シートが見つかりません: {e}")
        return

    # スプレッドシート上の既存コード
    existing_codes = []
    for attempt in range(3):
        try:
            existing_codes = sheet.col_values(3)
            break
        except Exception as e:
            print(f"警告: シートの読み込みに失敗しました ({attempt+1}/3). 10秒後にリトライします: {e}")
            time.sleep(10)

    # 重複チェック用セット（スプレッドシート + Supabase）
    all_existing = set(existing_codes)
    if supabase_existing:
        all_existing |= supabase_existing

    print(f"URLスキャン: {url}")
    try:
        results = scrape_pokecabook_results(url)[:MAX_DECKS_PER_RUN]
    except Exception as e:
        print(f"エラー: リンクの取得に失敗しました: {e}")
        return

    print(f"処理対象: {len(results)} 件")

    batch_rows = []      # スプレッドシート用
    supabase_batch = []  # Supabase用

    for item in results:
        code, rank = item['code'], item['rank']
        if code in all_existing: continue

        wait_time = random.uniform(5, 20)
        print(f"待機中 ({wait_time:.1f}秒)...")
        time.sleep(wait_time)

        cards = None
        for attempt in range(3):
            try:
                cards = fetch_deck_from_official(code)
                if cards: break
            except Exception as e:
                print(f"警告: データ取得エラー ({code}) [{attempt+1}/3]. 10秒後にリトライします...")
                time.sleep(10)

        if cards:
            final_rank = rank if rank != "不明" else "ALL"
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # スプレッドシート行
            batch_rows.append([
                str(uuid.uuid4()), USER_ID, code, archetype_id,
                json.dumps(cards, ensure_ascii=False),
                now_str,
                final_rank,
                item.get('event_date', ''),
                item.get('event_location', ''),
            ])

            # Supabase 行
            supabase_batch.append({
                'deck_code': code,
                'archetype_id': archetype_id,
                'cards_json': cards,
                'event_rank': final_rank,
            })

            # 既存セットに追加（同一実行内の重複防止）
            all_existing.add(code)
            if supabase_existing is not None:
                supabase_existing.add(code)

            print(f"完了: {code} | {final_rank} | {item.get('event_date','')} {item.get('event_location','')}")

            # 10件溜まったら一括書き込み
            if len(batch_rows) >= 10:
                save_batch(sheet, batch_rows)
                supabase_upsert_decks(supabase_batch)
                batch_rows = []
                supabase_batch = []
        else:
            print(f"失敗: {code}")

    # 残りを書き込み
    if batch_rows:
        save_batch(sheet, batch_rows)
        supabase_upsert_decks(supabase_batch)

def save_batch(sheet, rows):
    for attempt in range(3):
        try:
            sheet.append_rows(rows)
            print(f"--- {len(rows)}件をスプレッドシートへ一括保存しました！ ---")
            return
        except Exception as e:
            print(f"警告: 書き込みに失敗しました ({attempt+1}/3). 15秒後にリトライします: {e}")
            time.sleep(15)

if __name__ == "__main__":
    run_all_scrapers()

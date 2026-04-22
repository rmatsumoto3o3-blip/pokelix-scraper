"""
backfill.py
analyzed_decks シートのうち event_date / event_location が空の行を
pokecabook の元記事を再スクレイプして埋めるスクリプト。
一回限りの補完用。
"""

import time
import random
import gspread
from gspread import Cell
from oauth2client.service_account import ServiceAccountCredentials
from deck_parser import scrape_pokecabook_results
import os

SPREADSHEET_NAME = 'Pokelix DB'

# analyzed_decks の列インデックス（0始まり）
COL_DECK_CODE    = 2
COL_ARCHETYPE_ID = 3
COL_EVENT_RANK   = 6
COL_EVENT_DATE   = 7   # H列
COL_EVENT_LOC    = 8   # I列

def setup_gspread():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)

def run_backfill():
    client = setup_gspread()
    ss = client.open(SPREADSHEET_NAME)

    analyzed_sheet = ss.worksheet('analyzed_decks')
    config_sheet   = ss.worksheet('bot_config')
    arch_sheet     = ss.worksheet('deck_archetypes')

    # ── 1. analyzed_decks 全行読み込み ──────────────────────────
    all_rows = analyzed_sheet.get_all_values()
    header   = all_rows[0]
    data     = all_rows[1:]  # 2行目以降

    # H・I列がなければ空文字として扱う
    def get_col(row, idx):
        return row[idx].strip() if len(row) > idx else ''

    # event_date が空の行を対象にする
    target_rows = []
    for i, row in enumerate(data):
        if not get_col(row, COL_EVENT_DATE):
            target_rows.append({
                'sheet_row': i + 2,         # スプレッドシートの実際の行番号（1始まり、ヘッダー分+1）
                'deck_code':    get_col(row, COL_DECK_CODE),
                'archetype_id': get_col(row, COL_ARCHETYPE_ID),
            })

    print(f'バックフィル対象: {len(target_rows)} 件 / 全 {len(data)} 件')
    if not target_rows:
        print('対象なし。終了。')
        return

    # ── 2. archetype_id → pokecabook URL のマップを作成 ─────────
    arch_data  = arch_sheet.get_all_values()
    name_to_id = {row[1]: row[0] for row in arch_data[1:] if len(row) >= 2}

    config_data     = config_sheet.get_all_values()
    arch_id_to_urls = {}  # archetype_id → [url1, url2, ...] 複数URL対応
    for row in config_data[1:]:
        if len(row) < 3: continue
        aname = row[0].strip()
        aid   = row[1].strip()
        aurl  = row[2].strip()
        if not aid or len(aid) < 10:
            aid = name_to_id.get(aname, '')
        if aid and aurl:
            if aid not in arch_id_to_urls:
                arch_id_to_urls[aid] = []
            if aurl not in arch_id_to_urls[aid]:
                arch_id_to_urls[aid].append(aurl)

    # ── 3. 対象アーキタイプのURLを再スクレイプ ──────────────────
    arch_ids_needed = set(r['archetype_id'] for r in target_rows if r['archetype_id'])
    code_to_event   = {}  # deck_code → { event_date, event_location }

    total_archs = len(arch_ids_needed)
    for idx, arch_id in enumerate(arch_ids_needed, 1):
        urls = arch_id_to_urls.get(arch_id)
        if not urls:
            print(f'[{idx}/{total_archs}] URL不明: archetype_id={arch_id} → スキップ')
            continue

        for url in urls:
            print(f'[{idx}/{total_archs}] スクレイプ中: {url}')
            try:
                results = scrape_pokecabook_results(url)
                for item in results:
                    code = item['code']
                    if code and (item['event_date'] or item['event_location']):
                        code_to_event[code] = {
                            'event_date':     item['event_date'],
                            'event_location': item['event_location'],
                        }
                print(f'  → {len(results)} 件取得')
            except Exception as e:
                print(f'  → エラー: {e}')

            wait = random.uniform(5, 15)
            print(f'  待機 {wait:.1f}秒...')
            time.sleep(wait)

    print(f'\nコード→イベント情報マッピング完了: {len(code_to_event)} 件')

    # ── 4. 書き戻し対象を決定 ────────────────────────────────────
    cells_to_update = []
    matched = 0

    for r in target_rows:
        info = code_to_event.get(r['deck_code'])
        if not info:
            continue
        row_num = r['sheet_row']
        cells_to_update.append(Cell(row_num, COL_EVENT_DATE + 1, info['event_date']))      # H列
        cells_to_update.append(Cell(row_num, COL_EVENT_LOC  + 1, info['event_location']))  # I列
        matched += 1

    print(f'書き戻し対象: {matched} 件')

    if not cells_to_update:
        print('マッチしたデッキなし。終了。')
        return

    # ── 5. 一括更新（バッチAPIで1回）────────────────────────────
    # gspread は一度に1000セルまで update_cells できる
    BATCH_SIZE = 1000
    for i in range(0, len(cells_to_update), BATCH_SIZE):
        batch = cells_to_update[i:i + BATCH_SIZE]
        analyzed_sheet.update_cells(batch, value_input_option='RAW')
        print(f'  {i + len(batch)} / {len(cells_to_update)} セル更新完了')
        time.sleep(2)

    print(f'\n✅ バックフィル完了: {matched} 行に event_date / event_location を書き込みました')

if __name__ == '__main__':
    run_backfill()

"""
collect_urls.py
各アーキタイプ名でポケカBookを検索して、
「デッキレシピまとめ」系の記事URLを全件収集し
スプレッドシートの bot_config に追記する。
"""

import re
import time
import random
import os
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_NAME = 'Pokelix DB'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# まとめ記事と判断するキーワード（タイトルに含まれていれば対象）
SUMMARY_KEYWORDS = ['まとめ', 'レシピ', '環境', '優勝']

def setup_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)

def search_pokecabook(keyword):
    """ポケカBookでキーワード検索して記事URL一覧を返す"""
    url = f'https://pokecabook.com/?s={requests.utils.quote(keyword)}'
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200:
            return []
    except Exception as e:
        print(f'  検索エラー: {e}')
        return []

    soup = BeautifulSoup(res.text, 'html.parser')
    found = []

    # 記事リンクを探す（archives/数字 の形式）
    for a in soup.find_all('a', href=re.compile(r'pokecabook\.com/archives/\d+')):
        href  = a.get('href', '').strip().rstrip('/')
        title = a.get_text().strip()

        if not href or not title:
            continue
        # まとめ系キーワードを含む記事のみ
        if any(kw in title for kw in SUMMARY_KEYWORDS):
            found.append({'url': href, 'title': title})

    # 重複除去
    seen = set()
    unique = []
    for item in found:
        if item['url'] not in seen:
            seen.add(item['url'])
            unique.append(item)
    return unique

def run():
    client = setup_gspread()
    ss = client.open(SPREADSHEET_NAME)

    config_sheet = ss.worksheet('bot_config')
    config_data  = config_sheet.get_all_values()

    # 既存のURL一覧を収集（重複追加しないため）
    existing_urls = set(row[2].strip() for row in config_data[1:] if len(row) > 2)

    # 既存アーキタイプ一覧（name, id, url, active）
    existing_archs = {}
    for row in config_data[1:]:
        if len(row) < 2: continue
        aname = row[0].strip()
        aid   = row[1].strip()
        if aname and aid:
            existing_archs[aname] = aid

    print(f'既存アーキタイプ数: {len(existing_archs)}')
    print(f'既存URL数: {len(existing_urls)}')
    print()

    new_rows = []

    for idx, (aname, aid) in enumerate(existing_archs.items(), 1):
        print(f'[{idx}/{len(existing_archs)}] 検索中: {aname}')

        results = search_pokecabook(aname)
        added = 0
        for item in results:
            if item['url'] not in existing_urls:
                new_rows.append([aname, aid, item['url'], 'TRUE'])
                existing_urls.add(item['url'])
                print(f'  追加: {item["url"]}  ({item["title"][:40]})')
                added += 1

        if added == 0:
            print(f'  新規URLなし')

        wait = random.uniform(3, 8)
        print(f'  待機 {wait:.1f}秒...')
        time.sleep(wait)

    print(f'\n合計 {len(new_rows)} 件の新規URLを発見')

    if new_rows:
        config_sheet.append_rows(new_rows)
        print(f'✅ bot_config に {len(new_rows)} 行追記しました')
    else:
        print('追記なし（全URL既存）')

if __name__ == '__main__':
    run()

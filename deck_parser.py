import re
import requests
from bs4 import BeautifulSoup

# 通信時に「本物のブラウザ」だと思わせるための設定
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def fetch_deck_from_official(deck_code):
    """
    ポケカ公式サイトのデッキコードからカードリストを取得する
    """
    url = f"https://www.pokemon-card.com/deck/confirm.html/deckID/{deck_code}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return None

    html = response.text

    # PCGDECKオブジェクトのデータを抽出
    names = dict(re.findall(r"PCGDECK\.searchItemName\[(\d+)\]='([^']+)';", html))
    picts = dict(re.findall(r"PCGDECK\.searchItemCardPict\[(\d+)\]='([^']+)';", html))
    alt_names = dict(re.findall(r"PCGDECK\.searchItemNameAlt\[(\d+)\]='([^']+)';", html))

    input_type_map = {
        'deck_pke':  ('Pokémon', []),
        'deck_gds':  ('Trainer', ['Item']),
        'deck_tool': ('Trainer', ['Pokémon Tool']),
        'deck_sup':  ('Trainer', ['Supporter']),
        'deck_sta':  ('Trainer', ['Stadium']),
        'deck_ene':  ('Energy',  ['Basic']),
        'deck_tech': ('Trainer', ['Item']),
        'deck_ajs':  ('Trainer', ['Item']),
    }

    cards = []
    for input_id, (supertype, subtypes) in input_type_map.items():
        # hidden inputのvalueを取得
        match = re.search(rf'id=["\']{input_id}["\'][^>]*value=["\']([^"\']+)["\']', html)
        if not match:
            # 順番が逆の場合
            match = re.search(rf'value=["\']([^"\']+)["\'][^>]*id=["\']{input_id}["\']', html)

        if match:
            val = match.group(1)
            # 形式: "id_quantity_index-id_quantity_index"
            entries = val.split('-')
            for entry in entries:
                parts = entry.split('_')
                if len(parts) >= 2:
                    cid = parts[0]
                    quantity = int(parts[1])

                    if cid in picts:
                        cards.append({
                            "name": alt_names.get(cid) or names.get(cid, "Unknown"),
                            "imageUrl": f"https://www.pokemon-card.com{picts[cid]}",
                            "quantity": quantity,
                            "supertype": supertype,
                            "subtypes": subtypes,
                        })
    return cards

# ランク定義（長い文字列を先に並べること）
VALID_RANKS = ['TOP128', 'TOP64', 'TOP32', 'TOP16', 'TOP8', 'TOP4', '準優勝', '優勝']

def parse_event_text(text):
    """
    テキストから日付・場所・ランクを抽出する
    例1: "4/19【日】BOOKOFF　イオン橋本（神奈川）TOP4"
    例2: "チャンピオンズリーグ2026愛知 TOP16"
    """
    # ランク抽出（長い文字列から先に）
    rank = None
    for r in VALID_RANKS:
        if r in text:
            rank = r
            break

    # 日付抽出: 先頭の "4/19" など
    date_match = re.match(r'^(\d{1,2}/\d{1,2})', text)
    event_date = date_match.group(1) if date_match else ''

    # 場所抽出: 「日付【曜日】」プレフィックスとランクサフィックスを除去
    location = re.sub(r'^\d{1,2}/\d{1,2}【[^】]*】\s*', '', text)
    for r in VALID_RANKS:
        location = re.sub(re.escape(r) + r'\s*$', '', location).strip()
    location = location.strip()

    return {
        'event_date': event_date,
        'event_location': location,
        'rank': rank
    }


def scrape_pokecabook_results(url):
    """
    ポケカブックのまとめ記事から(日付, 場所, ランク, デッキコード)のリストを抽出する

    HTML構造パターン1（通常大会）:
      <figcaption class="wp-element-caption">
        4/19【日】アリオン塩冶店（島根）<a href="...deckID/G4cGxY...">TOP4</a>
      </figcaption>

    HTML構造パターン2（チャンピオンズリーグ等）:
      <p>チャンピオンズリーグ2026愛知 <a href="...deckID/xxxxx...">TOP16</a></p>
    """
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return []

    html = response.text
    results = []
    seen_codes = set()

    # --- パターン1: figcaption ---
    parts = html.split('<figcaption class="wp-element-caption">')
    for part in parts[1:]:
        end_idx = part.find('</figcaption>')
        if end_idx == -1:
            continue

        caption = part[:end_idx]
        code_match = re.search(r'deckID/([a-zA-Z0-9-]+)', caption)
        if not code_match:
            continue
        code = code_match.group(1)
        if code in seen_codes:
            continue

        text = re.sub(r'<[^>]+>', '', caption)
        text = re.sub(r'【[月火水木金土日]】', '', text).strip()
        cleaned = re.sub(r'平均化|平均レシピ|平均', '', text).strip()
        if not cleaned:
            continue

        parsed = parse_event_text(cleaned)
        seen_codes.add(code)
        results.append({
            'code': code,
            'rank': parsed['rank'] or '不明',
            'event_date': parsed['event_date'],
            'event_location': parsed['event_location'],
        })

    # --- パターン2: figcaptionがない場合、全リンクをスキャン ---
    if not results:
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=re.compile(r'deckID/')):
            href = a.get('href', '')
            code_match = re.search(r'deckID/([a-zA-Z0-9-]+)', href)
            if not code_match:
                continue
            code = code_match.group(1)
            if code in seen_codes:
                continue

            # 親要素のテキスト全体を使う
            parent = a.parent
            text = parent.get_text(separator=' ').strip()
            text = re.sub(r'【[月火水木金土日]】', '', text).strip()
            cleaned = re.sub(r'平均化|平均レシピ|平均', '', text).strip()
            if not cleaned:
                continue

            # リンクテキスト自体がランクの場合もある
            link_text = a.get_text().strip()
            parsed = parse_event_text(cleaned)
            rank = parsed['rank']
            if not rank and link_text in VALID_RANKS:
                rank = link_text

            seen_codes.add(code)
            results.append({
                'code': code,
                'rank': rank or '不明',
                'event_date': parsed['event_date'],
                'event_location': parsed['event_location'],
            })

    return results

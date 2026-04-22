import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def setup():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    
    if not os.path.exists(creds_path):
        print("エラー: credentials.json が見つかりません。")
        return

    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    print("--- スプレッドシートを捜索中... ---")
    files = client.openall()
    if not files:
        print("エラー: 共有されているスプレッドシートが見つかりません。")
        print("client_emailをスプレッドシートの『共有』から追加しましたか？")
        return

    # 'Pokelix' を含むものを優先、なければ最初の一つ
    ss = files[0]
    for f in files:
        if 'Pokelix' in f.title:
            ss = f
            break
    
    print(f"接続成功: {ss.title}")

    print("--- ドラパルトexのIDを特定中... ---")
    try:
        arch_sheet = ss.worksheet('deck_archetypes')
        data = arch_sheet.get_all_values()
        target_name = 'ドラパルト' # ここを「ドラパルト」に変更
        target_id = ""
        found_names = []
        
        # 1行目はヘッダー [id, name, ...]
        for row in data:
            if len(row) > 1:
                found_names.append(row[1])
                if target_name in row[1]:
                    target_id = row[0]
                    break
        
        if not target_id:
            print(f"警告: '{target_name}' が見つかりませんでした。")
            print(f"【ヒント】現在シートにある名前の例: {', '.join(found_names[1:10])}...")
            return

        print(f"特定成功: {target_id}")

        # main.py を自動更新
        main_path = os.path.join(os.path.dirname(__file__), 'main.py')
        with open(main_path, 'r', encoding='utf-8') as f:
            content = f.read()

        import re
        content = re.sub(r"SPREADSHEET_NAME = '.+'", f"SPREADSHEET_NAME = '{ss.title}'", content)
        content = re.sub(r"TARGET_ARCHETYPE_ID = '.+'", f"TARGET_ARCHETYPE_ID = '{target_id}'", content)

        with open(main_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("\n--- 全ての設定が完了しました！ ---")
        print(f"main.py を {ss.title} / {target_id} 用に最適化しました。")
        print("以下のコマンドでスクレイピングを開始できます：")
        print("python3 main.py")

    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    setup()

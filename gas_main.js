/**
 * Pokelix 解析エンジン v5
 * analyzed_decks 列構成:
 * 0:id, 1:user_id, 2:deck_code, 3:archetype_id, 4:cards_json,
 * 5:created_at, 6:event_rank, 7:event_date, 8:event_location
 */

const CUTOFF_DATE = new Date('2026-01-23T00:00:00Z');
const RANK_LIST = ['優勝', '準優勝', 'TOP4', 'TOP8'];

// ============================================================
// メニュー
// ============================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🔥 Pokelix 管理ツール')
    .addItem('🚀 集計 → 同期をまとめて実行', 'main_process_all')
    .addSeparator()
    .addItem('📊 アーキタイプ別集計のみ', 'calculateArchetypeStats_run')
    .addItem('🌍 全体集計のみ', 'calculateGlobalStats_run')
    .addSeparator()
    .addItem('☁️ Supabaseへ同期のみ（統計）', 'sync_all_stats_to_supabase')
    .addItem('📋 Supabaseへ同期のみ（デッキ一覧）', 'sync_deck_records_run')
    .addToUi();
}

function calculateArchetypeStats_run() {
  calculateArchetypeStats(SpreadsheetApp.getActiveSpreadsheet());
}
function calculateGlobalStats_run() {
  calculateGlobalStats(SpreadsheetApp.getActiveSpreadsheet());
}
function sync_deck_records_run() {
  sync_deck_records_to_supabase(SpreadsheetApp.getActiveSpreadsheet());
}

// ============================================================
// メインプロセス
// ============================================================

function main_process_all() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try {
    console.log('▶ 集計開始');
    calculateArchetypeStats(ss);
    calculateGlobalStats(ss);

    console.log('▶ Supabase同期開始（統計）');
    sync_all_stats_to_supabase();

    console.log('✅ 集計・同期がすべて完了しました！');
  } catch (e) {
    console.error(e);
  }
}

// ============================================================
// 【解析1】アーキタイプ別集計
// ============================================================

function calculateArchetypeStats(ss) {
  const data = ss.getSheetByName('analyzed_decks').getDataRange().getValues();
  data.shift();

  const statsMap    = {};
  const deckCountMap = {};

  data.forEach(function(row) {
    const deckCode     = row[2];
    const archetypeId  = row[3];
    const cardsJsonStr = row[4];
    const createdAt    = row[5] ? new Date(row[5]) : null;
    const eventRank    = row[6] || 'ALL';

    if (!archetypeId || !cardsJsonStr || !createdAt) return;
    if (createdAt < CUTOFF_DATE) return;

    var cards;
    try { cards = JSON.parse(cardsJsonStr); } catch (e) { return; }

    const buckets = ['ALL'];
    if (RANK_LIST.indexOf(eventRank) !== -1) buckets.push(eventRank);

    buckets.forEach(function(rank) {
      const deckKey = archetypeId + '__' + rank;
      if (!deckCountMap[deckKey]) deckCountMap[deckKey] = new Set();
      deckCountMap[deckKey].add(deckCode);

      const seenInDeck = new Set();
      cards.forEach(function(card) {
        if (!card.name) return;
        const statKey = archetypeId + '__' + rank + '__' + card.name;
        if (!statsMap[statKey]) {
          statsMap[statKey] = {
            archetypeId:  archetypeId,
            rank:         rank,
            cardName:     card.name,
            imageUrl:     card.imageUrl || '',
            supertype:    card.supertype || 'Pokémon',
            subtypes:     card.subtypes  || [],
            adoptionCount: 0,
            totalQty:     0
          };
        }
        statsMap[statKey].totalQty += (parseInt(card.quantity) || 0);
        if (!seenInDeck.has(card.name)) {
          statsMap[statKey].adoptionCount++;
          seenInDeck.add(card.name);
        }
      });
    });
  });

  const output = [];
  for (var key in statsMap) {
    const s = statsMap[key];
    const deckKey    = s.archetypeId + '__' + s.rank;
    const totalDecks = deckCountMap[deckKey] ? deckCountMap[deckKey].size : 0;
    if (totalDecks === 0) continue;
    output.push([
      s.archetypeId, s.cardName, s.imageUrl,
      s.adoptionCount, totalDecks, s.totalQty,
      s.rank, s.supertype, JSON.stringify(s.subtypes)
    ]);
  }

  const sheet = ss.getSheetByName('archetype_card_stats');
  sheet.clearContents();
  sheet.appendRow([
    'archetype_id', 'card_name', 'image_url',
    'adoption_count', 'total_decks', 'total_qty',
    'event_rank', 'supertype', 'subtypes'
  ]);
  if (output.length > 0) sheet.getRange(2, 1, output.length, output[0].length).setValues(output);
  console.log('✅ アーキタイプ別集計完了: ' + output.length + '行');
}

// ============================================================
// 【解析2】全体集計（ALL + ランク別）
// ============================================================

function calculateGlobalStats(ss) {
  const data = ss.getSheetByName('analyzed_decks').getDataRange().getValues();
  data.shift();

  const statsMap    = {};
  const deckCountMap = {};

  data.forEach(function(row) {
    const deckCode     = row[2];
    const cardsJsonStr = row[4];
    const createdAt    = row[5] ? new Date(row[5]) : null;
    const eventRank    = row[6] || 'ALL';

    if (!cardsJsonStr || !createdAt) return;
    if (createdAt < CUTOFF_DATE) return;

    var cards;
    try { cards = JSON.parse(cardsJsonStr); } catch (e) { return; }

    const buckets = ['ALL'];
    if (RANK_LIST.indexOf(eventRank) !== -1) buckets.push(eventRank);

    buckets.forEach(function(rank) {
      if (!deckCountMap[rank]) deckCountMap[rank] = new Set();
      deckCountMap[rank].add(deckCode);

      const seenInDeck = new Set();
      cards.forEach(function(card) {
        if (!card.name) return;
        const statKey = rank + '__' + card.name;
        if (!statsMap[statKey]) {
          statsMap[statKey] = {
            rank:         rank,
            cardName:     card.name,
            imageUrl:     card.imageUrl || '',
            supertype:    card.supertype || 'Pokémon',
            subtypes:     card.subtypes  || [],
            adoptionCount: 0,
            totalQty:     0
          };
        }
        statsMap[statKey].totalQty += (parseInt(card.quantity) || 0);
        if (!seenInDeck.has(card.name)) {
          statsMap[statKey].adoptionCount++;
          seenInDeck.add(card.name);
        }
      });
    });
  });

  const output = [];
  for (var key in statsMap) {
    const s = statsMap[key];
    const totalDecks = deckCountMap[s.rank] ? deckCountMap[s.rank].size : 0;
    if (totalDecks === 0) continue;
    output.push([
      s.cardName, s.imageUrl,
      s.adoptionCount, totalDecks, s.totalQty,
      s.rank, s.supertype, JSON.stringify(s.subtypes)
    ]);
  }

  const sheet = ss.getSheetByName('global_card_stats');
  sheet.clearContents();
  sheet.appendRow([
    'card_name', 'image_url',
    'adoption_count', 'total_decks', 'total_qty',
    'event_rank', 'supertype', 'subtypes'
  ]);
  if (output.length > 0) sheet.getRange(2, 1, output.length, output[0].length).setValues(output);
  console.log('✅ グローバル集計完了: ' + output.length + '行');
}

// ============================================================
// 【同期1】統計データ → Supabase
// ============================================================

function sync_all_stats_to_supabase() {
  const props  = PropertiesService.getScriptProperties();
  const SB_URL = props.getProperty('SUPABASE_URL');
  const SB_KEY = props.getProperty('SUPABASE_KEY');

  if (!SB_URL || !SB_KEY) throw new Error('ScriptProperties に SUPABASE_URL / SUPABASE_KEY が設定されていません');

  const baseUrl = SB_URL.replace(/\/$/, '');
  const ss      = SpreadsheetApp.getActiveSpreadsheet();

  const authHeaders = {
    'apikey':        SB_KEY,
    'Authorization': 'Bearer ' + SB_KEY
  };
  const insertHeaders = {
    'apikey':        SB_KEY,
    'Authorization': 'Bearer ' + SB_KEY,
    'Content-Type':  'application/json',
    'Prefer':        'return=minimal'
  };

  var res;

  // --- 既存データ削除 ---
  console.log('🗑 既存統計データを削除中...');
  res = UrlFetchApp.fetch(
    baseUrl + '/rest/v1/archetype_card_stats?id=not.is.null',
    { method: 'delete', headers: authHeaders, muteHttpExceptions: true }
  );
  if (res.getResponseCode() >= 400) throw new Error('archetype_card_stats 削除失敗: ' + res.getContentText());

  res = UrlFetchApp.fetch(
    baseUrl + '/rest/v1/global_card_stats?id=not.is.null',
    { method: 'delete', headers: authHeaders, muteHttpExceptions: true }
  );
  if (res.getResponseCode() >= 400) throw new Error('global_card_stats 削除失敗: ' + res.getContentText());

  // --- archetype_card_stats 挿入 ---
  // 列: 0:archetype_id, 1:card_name, 2:image_url, 3:adoption_count,
  //     4:total_decks, 5:total_qty, 6:event_rank, 7:supertype, 8:subtypes
  const archData = ss.getSheetByName('archetype_card_stats').getDataRange().getValues();
  archData.shift();
  for (var i = 0; i < archData.length; i += 500) {
    const chunk = archData.slice(i, i + 500).map(function(row) {
      var subtypes;
      try { subtypes = JSON.parse(row[8] || '[]'); } catch(e) { subtypes = []; }
      return {
        archetype_id:   row[0],
        card_name:      row[1],
        image_url:      row[2] || null,
        adoption_count: parseInt(row[3]) || 0,
        total_decks:    parseInt(row[4]) || 0,
        total_qty:      parseInt(row[5]) || 0,
        event_rank:     row[6] || 'ALL',
        supertype:      row[7] || null,
        subtypes:       subtypes
      };
    });
    res = UrlFetchApp.fetch(
      baseUrl + '/rest/v1/archetype_card_stats',
      { method: 'post', headers: insertHeaders, payload: JSON.stringify(chunk), muteHttpExceptions: true }
    );
    if (res.getResponseCode() >= 400) throw new Error('archetype INSERT 失敗(' + i + '件目): ' + res.getContentText());
    console.log('Archetype: ' + Math.min(i + 500, archData.length) + ' / ' + archData.length + ' 件完了');
  }

  // --- global_card_stats 挿入 ---
  // 列: 0:card_name, 1:image_url, 2:adoption_count, 3:total_decks,
  //     4:total_qty, 5:event_rank, 6:supertype, 7:subtypes
  const globData = ss.getSheetByName('global_card_stats').getDataRange().getValues();
  globData.shift();
  for (var j = 0; j < globData.length; j += 500) {
    const chunk = globData.slice(j, j + 500).map(function(row) {
      var subtypes;
      try { subtypes = JSON.parse(row[7] || '[]'); } catch(e) { subtypes = []; }
      return {
        card_name:      row[0],
        image_url:      row[1] || null,
        adoption_count: parseInt(row[2]) || 0,
        total_decks:    parseInt(row[3]) || 0,
        total_qty:      parseInt(row[4]) || 0,
        event_rank:     row[5] || 'ALL',
        supertype:      row[6] || null,
        subtypes:       subtypes
      };
    });
    res = UrlFetchApp.fetch(
      baseUrl + '/rest/v1/global_card_stats',
      { method: 'post', headers: insertHeaders, payload: JSON.stringify(chunk), muteHttpExceptions: true }
    );
    if (res.getResponseCode() >= 400) throw new Error('global INSERT 失敗(' + j + '件目): ' + res.getContentText());
    console.log('Global: ' + Math.min(j + 500, globData.length) + ' / ' + globData.length + ' 件完了');
  }

  console.log('✅ 統計データのSupabase同期完了');
}

// ============================================================
// 【同期2】個別デッキメタデータ → Supabase (deck_records)
// ============================================================

function sync_deck_records_to_supabase(ss) {
  const props  = PropertiesService.getScriptProperties();
  const SB_URL = props.getProperty('SUPABASE_URL');
  const SB_KEY = props.getProperty('SUPABASE_KEY');

  if (!SB_URL || !SB_KEY) throw new Error('ScriptProperties に SUPABASE_URL / SUPABASE_KEY が設定されていません');

  const baseUrl = SB_URL.replace(/\/$/, '');

  // ignore-duplicates: (deck_code, archetype_id) が重複するレコードはスキップ
  // → Supabase 側に UNIQUE(deck_code, archetype_id) 制約が必要
  const upsertHeaders = {
    'apikey':        SB_KEY,
    'Authorization': 'Bearer ' + SB_KEY,
    'Content-Type':  'application/json',
    'Prefer':        'return=minimal,resolution=ignore-duplicates'
  };

  const allRows = ss.getSheetByName('analyzed_decks').getDataRange().getValues();
  allRows.shift();

  const seen    = new Set();
  const records = [];

  allRows.forEach(function(row) {
    const deckCode      = String(row[2] || '').trim();
    const archetypeId   = String(row[3] || '').trim();
    const eventRank     = String(row[6] || 'ALL').trim();
    // event_date: Googleシートが日付型で保存している場合、"M/D" 形式に変換
    var eventDateRaw = row[7];
    var eventDate = '';
    if (eventDateRaw instanceof Date && !isNaN(eventDateRaw.getTime())) {
      eventDate = (eventDateRaw.getMonth() + 1) + '/' + eventDateRaw.getDate();
    } else {
      eventDate = String(eventDateRaw || '').trim();
    }
    const eventLocation = String(row[8] || '').trim();
    const createdAtRaw  = row[5];

    if (!deckCode || !archetypeId) return;

    const key = deckCode + '__' + archetypeId;
    if (seen.has(key)) return;
    seen.add(key);

    var createdAt;
    try {
      const d = new Date(createdAtRaw);
      if (isNaN(d.getTime())) return;
      if (d < CUTOFF_DATE) return;
      createdAt = d.toISOString();
    } catch(e) { return; }

    records.push({
      deck_code:      deckCode,
      archetype_id:   archetypeId,
      event_rank:     eventRank || null,
      event_date:     eventDate || null,
      event_location: eventLocation || null,
      created_at:     createdAt
    });
  });

  console.log('deck_records 対象件数（重複除去済み）: ' + records.length);

  // DELETE は行わず、新規レコードのみ INSERT（既存はスキップ）
  var inserted = 0;
  for (var i = 0; i < records.length; i += 500) {
    const chunk = records.slice(i, i + 500);
    const res = UrlFetchApp.fetch(
      baseUrl + '/rest/v1/deck_records',
      { method: 'post', headers: upsertHeaders, payload: JSON.stringify(chunk), muteHttpExceptions: true }
    );
    if (res.getResponseCode() >= 400) throw new Error('deck_records INSERT 失敗(' + i + '件目): ' + res.getContentText());
    inserted += chunk.length;
    console.log('deck_records: ' + inserted + ' / ' + records.length + ' 件処理済み');
  }

  console.log('✅ deck_records 増分同期完了: ' + records.length + '件送信（既存重複はスキップ）');
}

import os
from dotenv import load_dotenv # 環境変数(.env)を読み込むために追加
from flask import Flask, render_template, request, jsonify
from markupsafe import escape

from waitress import serve

import arxiv
from metapub import PubMedFetcher
import requests
import json
from citeproc import Citation, CitationItem, CitationStylesStyle, CitationStylesBibliography, formatter
from citeproc.source.json import CiteProcJSON


# .envファイルから環境変数を読み込む (NCBI_API_KEYのため)
load_dotenv() 

app = Flask(__name__)

# 環境変数からNCBI APIキーを読み込む
NCBI_API_KEY = os.environ.get("NCBI_API_KEY") 

# PubMed取得用のインスタンス
if NCBI_API_KEY:
    # emailはNCBIの推奨事項です
    fetcher = PubMedFetcher(api_key=NCBI_API_KEY, email="citation-tool-contact@example.com") 
    print("NCBI API Key loaded successfully.")
else:
    fetcher = PubMedFetcher()
    print("WARNING: NCBI API Key not found. You might hit rate limits.")

# CSLファイルを保存するディレクトリ
CSL_DIR = 'csl_styles'
if not os.path.exists(CSL_DIR):
    os.makedirs(CSL_DIR)

def get_csl_path(style_name):
    """
    指定されたスタイルのCSLファイルパスを返す。ローカルになければ公式GitHubからダウンロードする。
    """
    file_path = os.path.join(CSL_DIR, f"{style_name}.csl")
    
    if not os.path.exists(file_path):
        # CSL公式リポジトリからrawデータを取得
        url = f"https://raw.githubusercontent.com/citation-style-language/styles/master/{style_name}.csl"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded CSL style: {style_name}")
            else:
                print(f"Style '{style_name}' not found (Status {response.status_code}). Using 'nature'.")
                return get_csl_path('nature')
        except Exception as e:
            print(f"Download Error: {e}")
            return None
            
    return file_path


def process_given_name(given_name):
    """
    given nameを適切に処理する
    - スペースで分割
    - 小文字で始まるパート（例: 'von'）: そのまま保持
    - 大文字のみのパート（例: 'CS', 'AV'）: イニシャル化 ('C. S.', 'A. V.')
    - 大文字小文字混合のパート（例: 'Bartheld'）: 最初に現れたものにカンマを付ける
    
    例:
    'Bartheld CS' -> 'Bartheld, C. S.'
    'von Bartheld CS' -> 'von Bartheld, C. S.'
    'AV' -> 'A. V.'
    'John Paul GP' -> 'John, Paul G. P.'
    """
    if not given_name:
        return ""
    
    # スペースで分割
    parts = given_name.split()
    processed_parts = []
    comma_inserted = False
    
    for part in parts:
        if not part:
            continue
        
        # 小文字で始まる場合（例: 'von', 'de', 'van'など）
        if part[0].islower():
            processed_parts.append(part)
        # 大文字のみで構成されているかチェック
        elif part.isupper() and part.isalpha():
            # カンマがまだ挿入されていない場合、最後の混合パートの後に挿入
            if not comma_inserted and processed_parts:
                # 最後の要素にカンマを追加
                processed_parts[-1] = processed_parts[-1] + ','
                comma_inserted = True
            # イニシャル化: 'CS' -> 'C. S.'
            initials = '. '.join(list(part)) + '.'
            processed_parts.append(initials)
        else:
            # 大文字小文字混合の場合
            processed_parts.append(part)
    
    # 最後まで混合パートが見つからなかった場合（全てイニシャルの場合）
    # または最後のパートが混合の場合、カンマは不要
    
    return ' '.join(processed_parts)


def fetch_paper_data_csl(source_type, paper_id):
    """
    citeproc-py (CSL-JSON) が解釈できる形式でデータを返す
    given nameの処理を改善
    family nameの接頭辞（von, van, de等）を適切に処理
    """
    data = {}
    try:
        # --- データ取得ロジック ---
        if source_type == 'pubmed':
            article = fetcher.article_by_pmid(paper_id)
            authors_list = []
            for author_name in article.authors:
                if not author_name:
                    continue
                parts = author_name.split(' ')
                
                #print(f"PubMed author raw: '{author_name}' -> parts: {parts}")
                
                if len(parts) >= 2:
                    # PubMedの形式: "Family Given" または "von Family Given"
                    # 小文字で始まる部分を検出してfamily nameの接頭辞として扱う
                    
                    family_parts = []
                    given_start_idx = 0
                    
                    # 前から見ていって、最初の大文字始まりまでがfamily nameの一部
                    for i, part in enumerate(parts):
                        if part and part[0].isupper():
                            # 最初の大文字始まりがfamily nameの主要部分
                            # その前の小文字始まり部分は接頭辞
                            given_start_idx = i + 1
                            
                            # 接頭辞がある場合
                            if i > 0:
                                non_dropping = ' '.join(parts[:i])
                                family = part
                                given = ' '.join(parts[i + 1:]) if len(parts) > i + 1 else ""
                                
                                processed_given = process_given_name(given)
                                authors_list.append({
                                    "family": family,
                                    "given": processed_given,
                                    "non-dropping-particle": non_dropping
                                })
                            else:
                                # 接頭辞がない通常のケース
                                family = part
                                given = ' '.join(parts[i + 1:]) if len(parts) > i + 1 else ""
                                
                                processed_given = process_given_name(given)
                                authors_list.append({
                                    "family": family,
                                    "given": processed_given
                                })
                            break
                    else:
                        # 全て小文字始まりの場合（ありえないが念のため）
                        family = parts[0]
                        given = ' '.join(parts[1:]) if len(parts) > 1 else ""
                        processed_given = process_given_name(given)
                        authors_list.append({
                            "family": family,
                            "given": processed_given
                        })
                        
                elif len(parts) == 1:
                    authors_list.append({"family": parts[0], "given": ""})
            
            data = {
                "id": paper_id,
                "type": "article-journal",
                "title": article.title,
                "container-title": article.journal,
                "volume": article.volume,
                "issue": article.issue,
                "page": article.pages,
                "author": authors_list,
                "issued": {"date-parts": [[int(article.year)]]}
            }
            #print(data)
        
        elif source_type == 'arxiv':
            search = arxiv.Search(id_list=[paper_id])
            paper = next(search.results())
            authors_list = []
            for author in paper.authors:
                parts = author.name.split(' ')
                if len(parts) >= 2:
                    # family nameの接頭辞を検出
                    family_parts = []
                    given_parts = []
                    
                    # 後ろから処理: 最後が family name
                    family_idx = len(parts) - 1
                    
                    # 最後から2番目以降で小文字始まりがあればそれも family name の一部
                    while family_idx > 0 and parts[family_idx - 1][0].islower():
                        family_idx -= 1
                    
                    # family name部分
                    family_parts = parts[family_idx:]
                    # given name部分
                    given_parts = parts[:family_idx]
                    
                    given = ' '.join(given_parts)
                    processed_given = process_given_name(given)
                    
                    # 小文字始まりの接頭辞がある場合
                    if len(family_parts) > 1 and family_parts[0][0].islower():
                        author_entry = {
                            "family": family_parts[-1],  # 実際のfamily name
                            "given": processed_given,
                            "non-dropping-particle": ' '.join(family_parts[:-1])  # 接頭辞（von, van等）
                        }
                    else:
                        author_entry = {
                            "family": ' '.join(family_parts),
                            "given": processed_given
                        }
                    
                    authors_list.append(author_entry)
                elif len(parts) == 1:
                    authors_list.append({"family": parts[0], "given": ""})
            
            data = {
                "id": paper_id,
                "type": "article-journal",
                "title": paper.title,
                "container-title": "arXiv preprint",
                "volume": f"arXiv:{paper_id}",
                "author": authors_list,
                "issued": {"date-parts": [[paper.published.year, paper.published.month, paper.published.day]]},
                "URL": paper.entry_id
            }

        elif source_type == 'biorxiv':
            url = f"https://api.biorxiv.org/details/biorxiv/{paper_id}"
            resp = requests.get(url).json()
            if resp.get('messages') and resp['messages'][0]['status'] == 'ok':
                item = resp['collection'][-1]
                authors_list = []
                for auth in item['authors'].split(';'):
                    parts = auth.strip().split(' ')
                    if len(parts) >= 2:
                        # family nameの接頭辞を検出
                        family_idx = len(parts) - 1
                        while family_idx > 0 and parts[family_idx - 1][0].islower():
                            family_idx -= 1
                        
                        family_parts = parts[family_idx:]
                        given_parts = parts[:family_idx]
                        
                        given = ' '.join(given_parts)
                        processed_given = process_given_name(given)
                        
                        if len(family_parts) > 1 and family_parts[0][0].islower():
                            author_entry = {
                                "family": family_parts[-1],
                                "given": processed_given,
                                "non-dropping-particle": ' '.join(family_parts[:-1])
                            }
                        else:
                            author_entry = {
                                "family": ' '.join(family_parts),
                                "given": processed_given
                            }
                        
                        authors_list.append(author_entry)
                    elif len(parts) == 1:
                        authors_list.append({"family": parts[0], "given": ""})

                data = {
                    "id": paper_id,
                    "type": "article-journal",
                    "title": item['title'],
                    "container-title": "bioRxiv",
                    "page": item['doi'],
                    "author": authors_list,
                    "issued": {"date-parts": [[int(item['date'].split('-')[0])]]}
                }
            else:
                return None
        # --- データ取得ロジック終 ---
        
        return data

    except Exception as e:
        print(f"Error fetching {paper_id} ({source_type}): {e}")
        return None

# データを保存するデバッグディレクトリ (変更なし)
DEBUG_DIR = 'csl_debug_output'
if not os.path.exists(DEBUG_DIR):
    os.makedirs(DEBUG_DIR)

def process_csl(csl_json_data, style_name, citation_number=None):
    """
    citeproc-pyを使ってフォーマットする。
    修正版：formatterモジュールを正しく使用
    
    Args:
        csl_json_data: CSL-JSON形式のデータ
        style_name: CSLスタイル名
        citation_number: 通し番号（Noneの場合は番号を変更しない）
    """
    from citeproc import formatter
    import re
    
    csl_path = get_csl_path(style_name)
    if not csl_path:
        return "Style Load Error"

    item_id = csl_json_data.get('id', 'unknown_id')
    debug_prefix = os.path.join(DEBUG_DIR, f"{item_id}_{style_name}")

    try:
        # --- DEBUG 1: 入力データ ---
        with open(f"{debug_prefix}_1_input_data.json", 'w', encoding='utf-8') as f:
            json.dump(csl_json_data, f, indent=4, ensure_ascii=False)
        
        # 著者情報の詳細ログ
        #print(f"\n=== Processing {item_id} ===")
        #print(f"Authors in CSL data:")
        #for idx, author in enumerate(csl_json_data.get('author', [])):
            #print(f"  Author {idx + 1}:")
            #print(f"    family: {author.get('family', 'N/A')}")
            #print(f"    given: {author.get('given', 'N/A')}")
            #print(f"    non-dropping-particle: {author.get('non-dropping-particle', 'N/A')}")
        
        # 1. ソースの作成
        json_src = CiteProcJSON([csl_json_data])
        
        # 2. スタイルの読み込み
        bib_style = CitationStylesStyle(csl_path, validate=False)
        
        # 3. CitationStylesBibliographyの作成
        # formatter.htmlを渡す（formatterはモジュール、htmlはその属性）
        bibliography = CitationStylesBibliography(bib_style, json_src, formatter.html)
        
        # 4. Citationの作成と登録
        citation = Citation([CitationItem(item_id)])
        bibliography.register(citation)
        
        # 5. 書誌情報の生成
        # bibliography()は各エントリのリストを返す
        bib_entries = bibliography.bibliography()
        
        result_html = ""
        
        # bib_entriesの各要素を処理
        if bib_entries:
            for entry in bib_entries:
                # entryは文字列（整形されたHTML）として返される
                result_html += str(entry)
        
        # CSLが生成した生のHTMLをログ出力
        #print(f"\nCSL raw output for {item_id}:")
        #print(result_html[:500])  # 最初の500文字を表示
        with open(f"{debug_prefix}_2_csl_raw_output.html", 'w', encoding='utf-8') as f:
            f.write(result_html)
        
        # 結果が空の場合のエラー処理
        if not result_html.strip():
            #print(f"Warning: Empty result for {item_id}")
            
            # デバッグ情報を保存
            debug_info = {
                "item_id": item_id,
                "style": style_name,
                "csl_data": csl_json_data,
                "bib_entries_type": str(type(bib_entries)),
                "bib_entries_content": str(bib_entries) if bib_entries else "None"
            }
            with open(f"{debug_prefix}_3_debug_info.json", 'w', encoding='utf-8') as f:
                json.dump(debug_info, f, indent=4, ensure_ascii=False)
            
            return f"CSL Formatting produced no output for {item_id}"
        
        # 著者数をチェックして et al. の処理を行う
        author_count = len(csl_json_data.get('author', []))
        #print(f"Total authors in data: {author_count}")
        
        # CSLが著者を省略しているかチェック
        has_et_al = 'et al' in result_html.lower()
        
        # HTMLに含まれる著者数をカウント
        # family nameの出現回数を数える（完全一致）
        displayed_author_count = 0
        for author in csl_json_data.get('author', []):
            family = author.get('family', '')
            if family and family in result_html:
                displayed_author_count += 1
        
        #print(f"Displayed authors in HTML: {displayed_author_count}")
        #print(f"Has 'et al': {has_et_al}")
        
        # CSLが著者を省略したかの判定
        authors_omitted = (author_count > displayed_author_count)
        
        if has_et_al:
            # 既存の "et al." または "et al" を斜体化
            # パターン1: "et al."
            result_html = re.sub(
                r'\bet al\.',
                r'<i>et al.</i>',
                result_html,
                flags=re.IGNORECASE
            )
            # パターン2: "et al" (ピリオドなし)
            result_html = re.sub(
                r'\bet al\b(?!\.)',
                r'<i>et al.</i>',
                result_html,
                flags=re.IGNORECASE
            )
            #print("Italicized existing 'et al.'")
            
        elif authors_omitted:
            # et al. が含まれていないが、著者が省略されている場合
            # 自動的に et al. を追加（著者リストの直後）
            #print(f"Authors omitted ({author_count} -> {displayed_author_count}), adding 'et al.'")
            
            # 最後に表示されている著者のfamily nameとgiven nameを取得
            last_displayed_author_family = None
            last_displayed_author_given = None
            
            if displayed_author_count > 0:
                last_author_idx = displayed_author_count - 1
                if last_author_idx < len(csl_json_data.get('author', [])):
                    last_displayed_author_family = csl_json_data['author'][last_author_idx].get('family', '')
                    last_displayed_author_given = csl_json_data['author'][last_author_idx].get('given', '')
            
            if last_displayed_author_family and last_displayed_author_given:
                # given nameの処理されたバージョン（イニシャル化されている可能性がある）
                # HTMLに含まれるイニシャルを検出
                # 例: "Smith, J. A." の場合、"J. A." の後に挿入
                
                # given nameからイニシャル部分を抽出（ピリオドを含む）
                # 例: "J. A." や "John" など
                
                # 最も確実な方法: family nameの後に続くgiven name部分を探す
                # パターン: "FamilyName, GivenInitials" の後
                
                # family nameの位置を探す（最後の出現）
                family_pattern = re.escape(last_displayed_author_family)
                
                # family nameの後に続く部分（カンマとイニシャル）を検出
                # 例: "Smith, J. A." または "Smith, John"
                author_pattern = family_pattern + r',\s+([A-Z]\.?\s*)+\.?'
                
                matches = list(re.finditer(author_pattern, result_html))
                
                if matches:
                    # 最後のマッチを使用（同じ著者名が複数回出現する可能性があるため）
                    last_match = matches[-1]
                    insert_pos = last_match.end()
                    
                    # その位置に et al. を挿入
                    result_html = (
                        result_html[:insert_pos] + 
                        ' <i>et al.</i>' + 
                        result_html[insert_pos:]
                    )
                    #print(f"Inserted 'et al.' after given name (pattern match)")
                else:
                    # パターンマッチに失敗した場合の代替方法
                    # family nameの最後の出現位置を探し、その後の最初のピリオドの後に挿入
                    last_family_pos = result_html.rfind(last_displayed_author_family)
                    
                    if last_family_pos != -1:
                        # その位置以降で最初のピリオドを探す
                        # ただし、given nameの範囲内で探す（次の単語の前まで）
                        search_range = result_html[last_family_pos:last_family_pos + 100]
                        
                        # カンマの後のイニシャル部分を探す
                        # 例: ", J. A." の終わり
                        initial_pattern = r',\s+[A-Z]\.?(?:\s+[A-Z]\.?)*\.?'
                        initial_match = re.search(initial_pattern, search_range)
                        
                        if initial_match:
                            insert_pos = last_family_pos + initial_match.end()
                            result_html = (
                                result_html[:insert_pos] + 
                                ' <i>et al.</i>' + 
                                result_html[insert_pos:]
                            )
                            #print(f"Inserted 'et al.' after given name (fallback pattern)")
                        else:
                            # イニシャルパターンが見つからない場合
                            # family nameの後の最初のピリオドの後に挿入
                            period_pos = result_html.find('.', last_family_pos)
                            if period_pos != -1:
                                result_html = (
                                    result_html[:period_pos + 1] + 
                                    ' <i>et al.</i>' + 
                                    result_html[period_pos + 1:]
                                )
                                #print(f"Inserted 'et al.' after period (last fallback)")
                            else:
                                result_html = result_html.rstrip() + ' <i>et al.</i>'
                                #print(f"Inserted 'et al.' at end (no period found)")
                    else:
                        result_html = result_html.rstrip() + ' <i>et al.</i>'
                        #print(f"Inserted 'et al.' at end (family name not found)")
            else:
                # 著者情報が取得できない場合は末尾に追加
                result_html = result_html.rstrip() + ' <i>et al.</i>'
                #print(f"Inserted 'et al.' at end (no author info)")
        #else:
            #print("No author omission detected")
        
        
        # まず、CSLが生成した番号の後ろ（ピリオドの後）にタブを挿入
        # パターン1: "数字."の直後に文字が来る場合（例: "1.Author"）
        result_html = re.sub(r'(\d+\.)([A-Za-z])', r'\1\t\2', result_html)
        
        # パターン2: HTMLタグ内の番号（例: <div>1.</div>の後に続くテキスト）
        result_html = re.sub(r'(\d+\.</div>)([A-Za-z])', r'\1\t\2', result_html)
        
        # パターン3: 行頭の番号（スペースやタブなしで著者名が続く場合）
        result_html = re.sub(r'^(\d+\.)([A-Za-z])', r'\1\t\2', result_html, flags=re.MULTILINE)
        
        # 通し番号に置き換える処理（タブを目印にする）
        if citation_number is not None:
            # パターン1: "数字.\t" の形式を通し番号に置き換え
            result_html = re.sub(
                r'\d+\.\t',
                f'{citation_number}.\t',
                result_html
            )
            # パターン2: <div class="csl-left-margin">数字.</div> のような形式
            result_html = re.sub(
                r'(<div class="csl-left-margin">)\d+(\.?</div>)',
                rf'\g<1>{citation_number}\g<2>',
                result_html
            )
            # パターン3: 行頭の "数字. " や "数字.\t" 
            result_html = re.sub(
                r'^(\s*)\d+\.(\s)',
                rf'\g<1>{citation_number}.\g<2>',
                result_html,
                flags=re.MULTILINE
            )
            # パターン4: HTMLタグ直後の番号
            result_html = re.sub(
                r'(>)\s*\d+\.(\s)',
                rf'\g<1>{citation_number}.\g<2>',
                result_html
            )
        else: #citation number is None (=アルファベット順の場合)
            # パターン1: "数字.\t" の形式を削除
            result_html = re.sub(
                r'\d+\.\t',
                f'',
                result_html
            )
        
        # タブを視覚的に保持するためにHTMLエンティティに変換
        # 方法1: 複数のnon-breaking spaceで表現（4つのスペース相当）
        result_html = result_html.replace('\t', '&nbsp;&nbsp;&nbsp;&nbsp;')
        
        # 二重ピリオドを単一ピリオドに修正
        # パターン1: ".." を "." に
        result_html = re.sub(r'\.\.+', '.', result_html)
        # パターン2: ". ." のようなスペースを含むパターンも修正
        result_html = re.sub(r'\.\s+\.', '.', result_html)

        # --- DEBUG 4: 最終整形結果 ---
        with open(f"{debug_prefix}_4_final_html_output.html", 'w', encoding='utf-8') as f:
            f.write(result_html)
            
        return result_html

    except Exception as e:
        import traceback
        error_info = traceback.format_exc()
        with open(f"{debug_prefix}_error.log", 'w', encoding='utf-8') as f:
            f.write(error_info)
        #print(f"CSL Processing Error: {str(e)}")
        #print(error_info)
        return f"CSL Formatting Error for {item_id}: {str(e)}"


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate', methods=['POST'])
def generate():
    input_text = request.json.get('ids', '')
    style_name = request.json.get('style', 'nature')
    sort_alphabetically = request.json.get('sortAlphabetically', False)
    
    # 一時的に全データを格納するリスト
    citation_data = []
    
    for line in input_text.splitlines():
        line = line.strip()
        if not line: continue
        
        # ID判別ロジック
        if line.isdigit():
            source = 'pubmed'
        elif '10.1101' in line:
            source = 'biorxiv'
        elif '.' in line and len(line) < 15:
            source = 'arxiv'
        else:
            source = 'pubmed'

        csl_data = fetch_paper_data_csl(source, line)
        
        if csl_data:
            # ソート用のキーを取得（第一著者の姓）
            first_author_family = ""
            if csl_data.get('author') and len(csl_data['author']) > 0:
                first_author_family = csl_data['author'][0].get('family', '')
            
            citation_data.append({
                'csl_data': csl_data,
                'sort_key': first_author_family.lower(),
                'original_id': line,
                'error': False
            })
        else:
            citation_data.append({
                'csl_data': None,
                'sort_key': '',
                'original_id': line,
                'error': True
            })
    
    # アルファベット順ソートが指定されている場合
    if sort_alphabetically:
        # エラーでないものだけソート、エラーは最後に
        valid_citations = [c for c in citation_data if not c['error']]
        error_citations = [c for c in citation_data if c['error']]
        valid_citations.sort(key=lambda x: x['sort_key'])
        citation_data = valid_citations + error_citations
    
    # 結果リストに変換
    results = []
    citation_number = 1
    
    for item in citation_data:
        if item['error']:
            # エラーの場合
            if sort_alphabetically:
                # アルファベット順の場合は番号なし
                error_message = f"<span style='color:red'>Not Found or Fetch Error: {item['original_id']}</span>"
            else:
                # 通常の場合は番号付き
                error_message = f"{citation_number}. <span style='color:red'>Not Found or Fetch Error: {item['original_id']}</span>"
                citation_number += 1
            results.append(error_message)
        else:
            # 正常な引用文献の場合
            if sort_alphabetically:
                # アルファベット順の場合は通し番号を付けない（None を渡す）
                formatted_html = process_csl(item['csl_data'], style_name, citation_number=None)
            else:
                # 通常の場合は通し番号を付ける
                formatted_html = process_csl(item['csl_data'], style_name, citation_number)
                citation_number += 1
            
            results.append(formatted_html)
            #print(item['csl_data'])
            #print(formatted_html)

    return jsonify({'citations': results})

if __name__ == '__main__':
    # サーバーを起動
    #app.run(debug=False, port=5000)
    serve(app, host="127.0.0.1", port=5000)
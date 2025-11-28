# =========================================================
# Snowflake Cortex Handson シナリオ#2
# AIを用いた顧客の声分析アプリケーション
# Step6: Cortex Agent（Snowflake Intelligence）
# =========================================================
# 概要: Cortex Agentを使った統合AIアシスタント
# 特徴: RAG (Cortex Search) と データ分析 (Cortex Analyst) を統合
# 使用する機能: Snowflake Intelligence, Cortex Agent API
# =========================================================

import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# ページ設定
st.set_page_config(layout="wide")

# =========================================================
# Snowflakeセッション接続
# =========================================================
@st.cache_resource
def get_snowflake_session():
    """Snowflakeセッションを取得（キャッシュ付き）"""
    return get_active_session()

session = get_snowflake_session()

# =========================================================
# 設定値（定数）
# =========================================================
# Agent設定
AGENT_DATABASE = "SNOWFLAKE_INTELLIGENCE"
AGENT_SCHEMA = "AGENTS"
DEFAULT_AGENT_NAME = "SNOW_RETAIL_AGENT"

# API設定
AGENT_API_ENDPOINT = "/api/v2/cortex/agent:run"
AGENT_API_TIMEOUT = 60  # 秒

# セッション状態の初期化
if 'agent_chat_history' not in st.session_state:
    st.session_state.agent_chat_history = []

# =========================================================
# ユーティリティ関数
# =========================================================
def get_available_agents() -> list:
    """利用可能なAgentのリストを取得"""
    try:
        result = session.sql(f"""
            SHOW AGENTS IN SCHEMA {AGENT_DATABASE}.{AGENT_SCHEMA}
        """).collect()
        agents = [row['name'] for row in result]
        return agents
    except Exception as e:
        st.warning(f"Agent一覧の取得に失敗: {str(e)}")
        return []

def execute_agent_query(agent_name: str, question: str) -> dict:
    """Cortex Agent APIを使用してクエリを実行"""
    try:
        # Agent APIリクエストの構築
        messages = [
            {
                "role": "user",
                "content": [{"type": "text", "text": question}]
            }
        ]
        
        request_body = {
            "messages": messages,
            "agent_name": f"{AGENT_DATABASE}.{AGENT_SCHEMA}.{agent_name}"
        }
        
        # Cortex Agent API呼び出し
        try:
            import _snowflake
            resp = _snowflake.send_snow_api_request(
                "POST",
                AGENT_API_ENDPOINT,
                {},
                {},
                request_body,
                None,
                AGENT_API_TIMEOUT * 1000,
            )
            
            if resp["status"] < 400:
                response_data = json.loads(resp["content"])
                
                # レスポンスの解析
                response_text = ""
                sql_query = ""
                result_data = None
                sources = []
                tool_used = None
                
                if "message" in response_data and "content" in response_data["message"]:
                    content_list = response_data["message"]["content"]
                    
                    for item in content_list:
                        if item["type"] == "text":
                            response_text += item["text"] + "\n\n"
                        elif item["type"] == "sql":
                            sql_query = item.get("statement", "")
                            tool_used = "Cortex Analyst"
                        elif item["type"] == "tool_results":
                            # Cortex Searchの結果など
                            tool_results = item.get("results", [])
                            for tr in tool_results:
                                if "source" in tr:
                                    sources.append(tr["source"])
                            tool_used = "Cortex Search"
                
                # SQLが生成された場合は実行
                if sql_query and sql_query.strip():
                    try:
                        result_data = session.sql(sql_query).to_pandas()
                    except Exception as sql_error:
                        return {
                            "success": False,
                            "response_text": response_text,
                            "sql": sql_query,
                            "data": None,
                            "sources": sources,
                            "tool_used": tool_used,
                            "message": f"SQL実行エラー: {str(sql_error)}"
                        }
                
                return {
                    "success": True,
                    "response_text": response_text.strip(),
                    "sql": sql_query,
                    "data": result_data,
                    "sources": sources,
                    "tool_used": tool_used,
                    "message": "正常に完了しました"
                }
            else:
                error_content = json.loads(resp["content"])
                error_msg = f"APIエラー (ステータス: {resp['status']}): {error_content.get('message', '不明なエラー')}"
                return {
                    "success": False,
                    "response_text": "",
                    "sql": "",
                    "data": None,
                    "sources": [],
                    "tool_used": None,
                    "message": error_msg
                }
        
        except ImportError:
            return {
                "success": False,
                "response_text": "",
                "sql": "",
                "data": None,
                "sources": [],
                "tool_used": None,
                "message": "Cortex Agent APIにアクセスできません。Streamlit in Snowflake環境で実行してください。"
            }
        
    except Exception as e:
        return {
            "success": False,
            "response_text": "",
            "sql": "",
            "data": None,
            "sources": [],
            "tool_used": None,
            "message": f"Agentエラー: {str(e)}"
        }

# =========================================================
# メインページ
# =========================================================
st.title("🤖 Step6: Cortex Agent")
st.header("Snowflake Intelligence 統合AIアシスタント")

st.markdown("""
このページでは、**Snowflake Intelligence (Cortex Agent)** を使用した統合AIアシスタントを体験できます。

**Cortex Agentの特徴:**
- 🔍 **Cortex Search**: 企業ドキュメントからの情報検索（RAG）
- 📊 **Cortex Analyst**: 売上データの分析・SQL生成
- 🧠 **自動ルーティング**: 質問内容に応じて最適なツールを自動選択

Step3-5で学んだ機能が、単一のエージェントに統合されています！
""")

# =========================================================
# サイドバー設定
# =========================================================
st.sidebar.header("⚙️ Agent設定")

# 利用可能なAgentの取得
available_agents = get_available_agents()

if available_agents:
    selected_agent = st.sidebar.selectbox(
        "使用するAgent:",
        available_agents,
        index=0 if DEFAULT_AGENT_NAME not in available_agents else available_agents.index(DEFAULT_AGENT_NAME),
        help="データ分析とドキュメント検索を統合したAgentを選択"
    )
    st.sidebar.success(f"✅ Agent: {selected_agent}")
else:
    selected_agent = None
    st.sidebar.error("❌ 利用可能なAgentがありません")
    st.sidebar.info("""
    **Agentの作成方法:**
    1. AI/ML Studioを開く
    2. 「Create Agent」をクリック
    3. 以下のツールを追加:
       - Cortex Search (snow_retail_search_service)
       - Cortex Analyst (sales_analysis_model.yaml)
    4. Agentを保存・デプロイ
    """)

st.sidebar.markdown("---")
st.sidebar.info("""
**Cortex Agentの仕組み:**
1. 🗣️ 自然言語で質問
2. 🧠 Agentが質問を分析
3. 🔧 最適なツールを自動選択
4. 📊 結果を統合して回答
""")

st.markdown("---")

# =========================================================
# Agent機能の説明
# =========================================================
st.subheader("🛠️ 利用可能なツール")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    #### 🔍 Cortex Search (RAG)
    **対応する質問例:**
    - 「返品ポリシーについて教えて」
    - 「商品の品質保証は？」
    - 「配送料金について」
    
    **特徴:**
    - 企業ドキュメントから検索
    - 根拠資料を明示
    - 正確な企業情報を提供
    """)

with col2:
    st.markdown("""
    #### 📊 Cortex Analyst
    **対応する質問例:**
    - 「売上TOP10の商品は？」
    - 「月別売上推移を見せて」
    - 「店舗とECの売上比較」
    
    **特徴:**
    - 自然言語からSQL生成
    - データを自動分析
    - グラフで可視化
    """)

st.markdown("---")

# =========================================================
# Agentチャット
# =========================================================
st.subheader("💬 Agentとの対話")

# Agent未設定の場合のガード
if not selected_agent:
    st.error("""
    ⚠️ **Agentが設定されていません**
    
    Cortex Agentを使用するには、事前にAI/ML StudioでAgentを作成・デプロイする必要があります。
    
    サイドバーの「Agentの作成方法」を参照してください。
    """)
    st.stop()

# チャット履歴の表示
if st.session_state.agent_chat_history:
    st.markdown("#### 💭 対話履歴")
    for i, message in enumerate(st.session_state.agent_chat_history):
        if message["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.write(message["content"])
        elif message["role"] == "agent":
            with st.chat_message("assistant", avatar="🤖"):
                st.write(message["content"])
                
                # ツール使用情報
                if "tool_used" in message and message["tool_used"]:
                    st.caption(f"🔧 使用ツール: {message['tool_used']}")
                
                # 分析結果の表示（Cortex Analyst）
                if "result" in message and message["result"].get("data") is not None:
                    df = message["result"]["data"]
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                
                # 生成されたSQLの表示
                if "result" in message and message["result"].get("sql"):
                    with st.expander("📝 生成されたSQL"):
                        st.code(message["result"]["sql"], language="sql")
                
                # 参照ドキュメントの表示（Cortex Search）
                if "result" in message and message["result"].get("sources"):
                    with st.expander("📚 参照ドキュメント"):
                        for j, source in enumerate(message["result"]["sources"], 1):
                            st.markdown(f"**{j}.** {source}")

# 質問入力エリア
col1, col2 = st.columns([4, 1])

with col1:
    user_question = st.text_input(
        "💬 何でも質問してください:",
        key="agent_input",
        placeholder="例: 売上TOP5の商品と返品ポリシーを教えて"
    )

with col2:
    st.write("")
    clear_chat = st.button("🗑️ クリア", help="チャット履歴をクリア")

# Agent実行処理
if st.button("🚀 Agentに質問", type="primary", use_container_width=True):
    if user_question:
        # ユーザー質問を履歴に追加
        st.session_state.agent_chat_history.append({
            "role": "user", 
            "content": user_question
        })
        
        with st.spinner("🧠 Agentが考え中..."):
            # Agent APIを実行
            result = execute_agent_query(selected_agent, user_question)
            
            if result["success"]:
                response_text = result.get("response_text", "回答を生成しました。")
                
                st.session_state.agent_chat_history.append({
                    "role": "agent",
                    "content": response_text,
                    "tool_used": result.get("tool_used"),
                    "result": result
                })
            else:
                error_message = f"申し訳ありません。処理中にエラーが発生しました。\n\n**エラー内容**: {result['message']}"
                st.session_state.agent_chat_history.append({
                    "role": "agent",
                    "content": error_message,
                    "tool_used": None,
                    "result": result
                })
        
        st.rerun()

# チャットクリア処理
if clear_chat:
    st.session_state.agent_chat_history = []
    st.rerun()

# =========================================================
# よくある質問テンプレート
# =========================================================
st.markdown("---")
st.subheader("💡 よくある質問テンプレート")

# 質問カテゴリ
question_templates = {
    "データ分析（Cortex Analyst）": [
        "売上TOP10の商品とその売上金額を教えて",
        "月別の売上推移を見せて",
        "店舗とECの売上を比較して",
        "商品カテゴリ別の売上ランキング"
    ],
    "ドキュメント検索（Cortex Search）": [
        "返品・交換のポリシーを教えて",
        "配送料金と配送時間について",
        "ポイントカードの特典は？",
        "スノーリテールの企業理念"
    ],
    "複合質問": [
        "売上TOP5商品の品質保証について教えて",
        "最も売れている商品カテゴリと関連するFAQは？",
        "ECの売上トレンドと顧客サービス方針"
    ]
}

tab1, tab2, tab3 = st.tabs(list(question_templates.keys()))

for tab, (category, questions) in zip([tab1, tab2, tab3], question_templates.items()):
    with tab:
        st.markdown(f"#### {category}")
        cols = st.columns(2)
        
        for i, question in enumerate(questions):
            with cols[i % 2]:
                if st.button(question, key=f"template_{category}_{i}", use_container_width=True):
                    st.session_state.agent_chat_history.append({
                        "role": "user",
                        "content": question
                    })
                    
                    with st.spinner("🧠 Agentが考え中..."):
                        result = execute_agent_query(selected_agent, question)
                        
                        if result["success"]:
                            response_text = result.get("response_text", "回答を生成しました。")
                            st.session_state.agent_chat_history.append({
                                "role": "agent",
                                "content": response_text,
                                "tool_used": result.get("tool_used"),
                                "result": result
                            })
                        else:
                            error_message = f"処理中にエラーが発生しました: {result['message']}"
                            st.session_state.agent_chat_history.append({
                                "role": "agent",
                                "content": error_message,
                                "tool_used": None,
                                "result": result
                            })
                    
                    st.rerun()

# =========================================================
# 統計情報
# =========================================================
st.markdown("---")
st.subheader("📊 Agent利用統計")

col1, col2, col3, col4 = st.columns(4)

total_messages = len(st.session_state.agent_chat_history)
user_questions = len([msg for msg in st.session_state.agent_chat_history if msg["role"] == "user"])
agent_responses = len([msg for msg in st.session_state.agent_chat_history if msg["role"] == "agent"])

# ツール使用統計
analyst_uses = len([msg for msg in st.session_state.agent_chat_history 
                   if msg["role"] == "agent" and msg.get("tool_used") == "Cortex Analyst"])
search_uses = len([msg for msg in st.session_state.agent_chat_history 
                  if msg["role"] == "agent" and msg.get("tool_used") == "Cortex Search"])

with col1:
    st.metric("💬 総メッセージ", f"{total_messages}件")

with col2:
    st.metric("❓ ユーザー質問", f"{user_questions}件")

with col3:
    st.metric("📊 Analyst使用", f"{analyst_uses}回")

with col4:
    st.metric("🔍 Search使用", f"{search_uses}回")

# =========================================================
# Step6 完了メッセージ
# =========================================================
st.markdown("---")
st.subheader("🎯 Step6 完了！")
st.success("""
✅ **Cortex Agent（Snowflake Intelligence）の実装が完了しました！**

**実装した機能:**
- Cortex SearchとCortex Analystの統合
- 質問内容に応じた自動ツール選択
- 自然言語による統合AIアシスタント
- 使用ツールの可視化と結果表示

**Step3-5との違い:**
- 個別機能 → 統合AIアシスタント
- 手動選択 → 自動ルーティング
- 単機能 → マルチツール対応

**ビジネス価値:**
- ユーザーは機能を意識せずに質問可能
- 1つのインターフェースで全機能にアクセス
- より自然な対話体験を提供
""")

st.info("🎉 **ハンズオン完了**: 全6ステップのSnowflake Cortex Handsonが完了しました！")

# フッター
st.markdown("---")
st.markdown("**Snowflake Cortex Handson シナリオ#2 | Step6: Cortex Agent**")


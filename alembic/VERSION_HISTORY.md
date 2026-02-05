# PE Org-AI-R Platform - Database Version History

## 📚 版本控制說明

本項目使用 Alembic 遷移腳本來記錄數據庫 schema 的演變歷史。雖然當前 Snowflake 與 Alembic 的自動化支持有限,但我們保留遷移腳本作為文檔和歷史記錄。

---

## 🗓️ 版本歷史

### Version 2.0 - Case Study 2 擴展 (2026-02-04)
**Revision:** `002_cs2_extensions`
**Revises:** `001_core_tables`

#### 新增表 (3 個)

5. **DOCUMENTS** - SEC 文檔管理
   - 10-K, 10-Q, 8-K 等文檔
   - 支持 S3 存儲和本地路徑
   - 處理狀態追蹤

6. **DOCUMENT_CHUNKS** - 文檔分塊
   - 用於向量檢索和 RAG
   - 支持分段和索引
   - 記錄字符位置

7. **EXTERNAL_SIGNALS** - 外部信號數據
   - LinkedIn 招聘信號
   - GitHub 創新活動
   - 數字存在度指標
   - 領導層信號
   - 使用 Snowflake VARIANT 存儲 metadata

#### 新增索引
- `idx_documents_company` - 按公司查詢文檔
- `idx_documents_status` - 按狀態過濾文檔
- `idx_chunks_document` - 按文檔查詢分塊
- `idx_signals_company` - 按公司查詢信號
- `idx_signals_category` - 按類別過濾信號

#### 遷移文件
📄 `alembic/versions/20260204_002_case_study_2_extensions.py`

---

### Version 1.0 - 初始核心表 (2026-02-04)
**Revision:** `001_core_tables`
**Revises:** None (初始版本)

#### 核心表 (4 個)

1. **INDUSTRIES** - 行業參考數據
   - 8 個 PE 行業分類
   - H^R baseline 參數

2. **COMPANIES** - 公司信息
   - 公司基本信息
   - 行業歸屬
   - Position factor (δ)
   - 軟刪除支持

3. **ASSESSMENTS** - 評估記錄
   - 4 種評估類型: screening, due_diligence, quarterly, exit_prep
   - 5 種狀態: draft, in_progress, submitted, approved, superseded
   - V^R score 和信心區間

4. **DIMENSION_SCORES** - AI 就緒度維度評分
   - 7 個維度:
     - data_infrastructure
     - ai_governance
     - technology_stack
     - talent_skills
     - leadership_vision
     - use_case_portfolio
     - culture_change
   - 權重配置
   - 信心度和證據計數

#### 初始索引
- `idx_companies_industry` - 公司-行業關聯
- `idx_companies_deleted` - 軟刪除過濾
- `idx_assessments_company` - 評估-公司關聯
- `idx_assessments_status` - 狀態過濾
- `idx_assessments_type` - 類型過濾
- `idx_dimension_scores_assessment` - 評分-評估關聯

#### 遷移文件
📄 `alembic/versions/20260204_001_initial_core_tables.py`

---

## 🔄 版本演變圖

```
v1.0 (001_core_tables)          v2.0 (002_cs2_extensions)
┌──────────────────────┐        ┌──────────────────────┐
│ ✓ INDUSTRIES         │   -->  │ ✓ INDUSTRIES         │
│ ✓ COMPANIES          │        │ ✓ COMPANIES          │
│ ✓ ASSESSMENTS        │        │ ✓ ASSESSMENTS        │
│ ✓ DIMENSION_SCORES   │        │ ✓ DIMENSION_SCORES   │
└──────────────────────┘        │ + DOCUMENTS          │
                                │ + DOCUMENT_CHUNKS    │
                                │ + EXTERNAL_SIGNALS   │
                                └──────────────────────┘
   4 tables                        7 tables
```

---

## 📊 當前狀態

- **當前版本**: v2.0 (002_cs2_extensions)
- **總表數**: 7
- **數據庫**: Snowflake
- **Schema**: PUBLIC

查詢當前版本:
```sql
SELECT version_num FROM alembic_version;
```

---

## 🔮 未來版本規劃

### Version 3.0 (計劃中)
- Company signal summaries 物化視圖
- 向量嵌入存儲 (vector embeddings)
- 審計日誌表
- 用戶權限管理

---

## 📝 注意事項

### Snowflake 特殊考量

1. **CHECK 約束**: Snowflake 不支持 CHECK 約束,所有驗證在應用層 (Pydantic) 完成
2. **VARIANT 類型**: 使用 Snowflake 的 VARIANT 類型而非標準 JSON
3. **TIMESTAMP**: 使用 TIMESTAMP_NTZ (無時區)
4. **表名大小寫**: Snowflake 默認將表名轉為大寫

### 手動版本管理

由於 Alembic 與 Snowflake 的兼容性限制,版本更新需要:
1. 執行遷移腳本中的 SQL
2. 手動更新 `alembic_version` 表

```sql
-- 更新到新版本
UPDATE alembic_version
SET version_num = '新版本號';
```

---

**生成日期**: 2026-02-04
**維護者**: PE Org-AI-R Platform Team

# SQL 轉換工具規格書

## 1. 目的

本工具用於離線讀取 `config.json` 所指定之 SQL 檔案中的資料，將其轉換為符合指定格式的新 SQL 與 CSV 輸出，並在發生錯誤時持續處理其餘資料，同時寫入 log。

## 2. 執行限制

- 執行環境必須為 `Python 3.9.10`
- 工具必須可離線執行
- 不可下載、安裝任何額外 modules
- 僅可使用 Python 標準函式庫

## 3. 設定檔

### 3.1 config.json

程式啟動時讀取 `config.json`（可透過 `--config` 參數指定其他路徑，預設為 `config.json`）。

必要欄位如下：

- `sql_file`：輸入 SQL 檔案路徑
- `zipcode_file`：zipcode.json 檔案路徑
- `output_dir`：輸出目錄路徑
- `table_name`：輸出 INSERT INTO 所使用的 table 名稱

任一欄位缺失時，程式印出錯誤訊息並立即結束。

## 4. 輸入檔案

### 4.1 必要檔案

- `config.json` 中 `sql_file` 指定的 SQL 檔案
- `config.json` 中 `zipcode_file` 指定的 zipcode JSON 檔案

### 4.2 輸入編碼

- 讀取輸入檔時，依序嘗試以下編碼：`Big5` → `CP950` → `UTF-8` → `UTF-8-BOM`
- 若上述四種編碼皆失敗，最後以 `CP950` 搭配 `errors=replace` 強制解碼（無法辨識的 bytes 以替換字元取代），並在 log 記錄 `WARN`
- 此設計確保單一檔案中有少數異常 bytes 時，仍可繼續處理整批資料

## 5. 輸出檔案

### 5.1 SQL 輸出

- 檔名格式：`updated_sql_yyyyMMddHHmmss.txt`
- 編碼：`Big5`

### 5.2 CSV 輸出

- 檔名格式：`updated_sql_yyyyMMddHHmmss.csv`
- 編碼：`Big5`
- 內容需包含每筆成功轉換後的 `VALUES` 欄位資料
- 第一列需輸出 header

### 5.3 Log 輸出

- 檔名格式：`log_yyyyMMdd.txt`
- 編碼可使用系統預設或 `UTF-8`
- 若同日重複執行，可附加寫入同一檔案

## 6. SQL 輸入格式假設

- 輸入 SQL 檔案（路徑由 `config.json` 的 `sql_file` 指定）主要內容為 `INSERT ... VALUES (...)` 類型 SQL
- 可接受單行一筆或多行組成一筆 SQL
- SQL 之間以分號 `;` 作為切分依據
- 檔案中若存在空白行、非 INSERT 內容、註解或無法解析的片段，程式不可中止，需記錄至 log 後略過

## 7. 固定輸出欄位結構

轉換後每筆 SQL 的欄位順序固定如下：

`ACCTNO,UID,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ADDRESS_1,ADDRESS_2,ADDRESS_3,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER`

轉換後每筆 SQL 的前綴格式為（table 名稱由 `config.json` 的 `table_name` 決定）：

```sql
INSERT INTO {table_name} (ACCTNO,UID,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ADDRESS_1,ADDRESS_2,ADDRESS_3,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER)
```

## 8. SQL 轉換規則

### 8.1 基本格式

- 每筆輸出 SQL 必須獨立一行
- 每筆輸出 SQL 必須以分號 `;` 結尾
- SQL 關鍵字需轉為大寫
- 表名與欄位名稱保持原樣，不因關鍵字轉換而修改大小寫

### 8.2 僅處理可辨識的 INSERT VALUES

- 僅處理可成功解析 `VALUES (...)` 的 SQL
- 若無法正確解析欄位值，需寫 log 並略過該筆

### 8.3 欄位對應

- `UID` 欄位固定寫入 `newID()`，忽略來源值
- 其餘欄位依原始資料欄位名稱對應填入

### 8.4 TRIM 規則

- `ACCTNO`、`USERNAME`、`ADDRESS_3` 三個欄位在轉換時須做前後 TRIM
- TRIM 範圍包含一般空白、Tab、換行，以及全形空白（U+3000）


## 9. CSV 輸出規格

### 9.1 欄位順序

CSV 欄位順序必須與下列欄位完全一致：

`ACCTNO,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ZIPCODE_ORIGIN,ADDRESS_1,ADDRESS_2,ADDRESS_3,ADDRESS_3_ORIGIN,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER,COMENTS`

### 9.2 內容來源

- 每筆成功轉換的 SQL，都需同步輸出一筆 CSV
- 因欄位含特殊字元而無法輸出 SQL 的記錄，仍須輸出一筆 CSV（詳見 Section 10）
- CSV 寫入內容應為轉換後最終欄位值，而非原始欄位值

## 10. 特殊字元處理規則

當某筆記錄的輸出 SQL 無法編碼為 Big5 時（例如欄位值含 `\ufffd` 等替換字元或其他非 Big5 字元）：

1. 逐一檢查 SQL 輸出欄位，找出所有無法轉 Big5 的欄位
2. 將問題欄位名稱寫入 CSV `COMENTS`，格式為：`{欄位名}欄位含特殊字元無法轉換`（多個欄位以 `；` 分隔）
3. 問題欄位的原始值**保留不清空**，供人工檢視（CSV 寫入時以 `?` 替代無法轉 Big5 的字元）
4. 該筆記錄**跳過 SQL 輸出**，但**仍寫入 CSV**
5. Log 記錄 `WARN`，說明是哪些欄位造成問題

此機制與輸入編碼的 `cp950+replace` 保底機制銜接：若 rogue bytes 被替換為 `\ufffd` 並落在某欄位值中，該欄位即會被此機制偵測並處理。

## 11. ZIPCODE 轉換規則

- `ADDRESS_3_ORIGIN` 無論 ZIPCODE 是否為 null，皆固定複製原始 `ADDRESS_3` 值

### 11.1 ZIPCODE 為 null

- 若 `ZIPCODE` 為 SQL `null` 或 空白，則：
  - `ZIPCODE` 設為空字串 `''`，`ADDRESS_1` 設為空字串 `''`，`ADDRESS_2` 設為空字串 `''`
    - COPY `null` or 空白 to `ZIPCODE_ORIGIN`
  - CSV `COMENTS` 欄位新增文字 `"客戶資料無zipcode"`

### 11.2 ZIPCODE 不為 null，且查無對應

- 若 `ZIPCODE` 不為 `null`，但在 `zipcode.json` 中找不到對應：
  - 保留 `ZIPCODE` 原值
  - 將ZIPCODE比對zipcode.json裏面的zipCode，
    - 如果有比對到，將city COPY to ADDRESS_1 and name copy to ADDRESS_2
    - 如果沒有比對到，ADDRESS_1 and ADDRESS_2 這定為空白 
  - CSV `COMENTS` 欄位新增文字 `"客戶zipcode資料不存在zipcode.json"`

## 12. GENDER 轉換規則

- `GENDER` 比對採大小寫不敏感
- 若 `GENDER` 為 `M` 或 `m`，轉為 `'1'`
- 若 `GENDER` 為 `F` 或 `f`，轉為 `'0'`
- 其他所有值（包含空字串、`null`、非預期字元），轉為空字串 `''`，並在 CSV `COMENTS` 欄位新增文字 `"GENDER欄位不正確"`

## 13. null 與值判定規則

- SQL 中的 `null` 與 `NULL` 視為空值
- 字串 `'null'` 不視為 SQL 空值，而是一般字串
- 空字串 `''` 不等於 SQL `null`
- 判斷 `ZIPCODE` 是否為空值時，應依 SQL 值型態判斷，不可單純以文字包含 `null` 判斷

## 14. 錯誤處理

- 任一筆資料轉換失敗時，不可中止整體程式
- 程式必須略過失敗資料並繼續處理下一筆
- 以下情況至少需寫入 log：
  - 輸入檔無法讀取
  - SQL 無法切分
  - SQL 非預期格式
  - `VALUES` 無法解析
  - 欄位數量不符
  - `zipcode.json` 無法讀取或解析
  - 單筆資料寫檔失敗

## 15. Log 記錄內容

每筆 log 至少應包含：

- 發生時間
- 錯誤等級，可至少區分 `ERROR` 與 `WARN`
- 原始資料所在行號或 SQL 序號
- 原始 SQL 摘要
- 錯誤原因

## 16. 時間規則

- 檔名中的時間使用執行當下本機時間
- `updated_sql_yyyyMMddHHmmss.*` 採 24 小時制
- `log_yyyyMMdd.txt` 使用同一本機日期

## 17. 驗收標準

- 可在未安裝任何第三方套件的 `Python 3.9.10` 環境執行
- 能離線完成轉換
- 能正確產出 SQL、CSV、log 三種檔案
- 遇到錯誤資料時不會整體中止
- `ZIPCODE` 與 `GENDER` 規則符合本規格
- 輸出檔編碼符合需求
- 含特殊字元無法轉 Big5 的記錄仍可輸出至 CSV，並於 `COMENTS` 標註問題欄位

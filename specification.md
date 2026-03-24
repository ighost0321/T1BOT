# SQL 轉換工具規格書

## 1. 目的

本工具用於離線讀取 `sql.txt` 中的 SQL 資料，將其轉換為符合指定格式的新 SQL 與 CSV 輸出，並在發生錯誤時持續處理其餘資料，同時寫入 log。

## 2. 執行限制

- 執行環境必須為 `Python 3.9.10`
- 工具必須可離線執行
- 不可下載、安裝任何額外 modules
- 僅可使用 Python 標準函式庫

## 3. 輸入檔案

### 3.1 必要檔案

- `sql.txt`
- `zipcode.json`

### 3.2 輸入編碼

- 讀取輸入檔時，必須優先嘗試 `Big5`
- 若 `Big5` 讀取失敗，再嘗試 `UTF-8`
- 若仍無法讀取，需記錄錯誤至 log，並結束該檔案處理流程

## 4. 輸出檔案

### 4.1 SQL 輸出

- 檔名格式：`updated_sql_yyyyMMddHHmmss.txt`
- 編碼：`Big5`

### 4.2 CSV 輸出

- 檔名格式：`updated_sql_yyyyMMddHHmmss.csv`
- 編碼：`Big5`
- 內容需包含每筆成功轉換後的 `VALUES` 欄位資料
- 第一列需輸出 header

### 4.3 Log 輸出

- 檔名格式：`log_yyyyMMdd.txt`
- 編碼可使用系統預設或 `UTF-8`
- 若同日重複執行，可附加寫入同一檔案

## 5. SQL 輸入格式假設

- `sql.txt` 主要內容為 `INSERT ... VALUES (...)` 類型 SQL
- 可接受單行一筆或多行組成一筆 SQL
- SQL 之間以分號 `;` 作為切分依據
- 檔案中若存在空白行、非 INSERT 內容、註解或無法解析的片段，程式不可中止，需記錄至 log 後略過

## 6. 固定輸出欄位結構

轉換後每筆 SQL 的欄位順序固定如下：

`ACCTNO,UID,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ADDRESS_1,ADDRESS_2,ADDRESS_3,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER`

轉換後每筆 SQL 的前綴固定為：

```sql
INSERT INTO account_info (ACCTNO,UID,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ADDRESS_1,ADDRESS_2,ADDRESS_3,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER)
```

## 7. SQL 轉換規則

### 7.1 基本格式

- 每筆輸出 SQL 必須獨立一行
- 每筆輸出 SQL 必須以分號 `;` 結尾
- SQL 關鍵字需轉為大寫
- 表名與欄位名稱保持原樣，不因關鍵字轉換而修改大小寫

### 7.2 僅處理可辨識的 INSERT VALUES

- 僅處理可成功解析 `VALUES (...)` 的 SQL
- 若無法正確解析欄位值，需寫 log 並略過該筆

### 7.3 欄位對應

- 原始 SQL 若缺少 `ADDRESS1`、`ADDRESS2` 欄位，轉換後需補入固定欄位位置
- UID欄位固定寫入newID()
- 其餘欄位依原始資料對應填入


## 8. CSV 輸出規格

### 8.1 欄位順序

CSV 欄位順序必須與下列欄位完全一致：

`ACCTNO,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,ZIPCODE,ZIPCODE_ORIGIN,ADDRESS_1,ADDRESS_2,ADDRESS_3,ADDRESS_3_ORIGIN,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER,COMENTS`

### 8.2 內容來源

- 每筆成功轉換的 SQL，都需同步輸出一筆 CSV
- CSV 寫入內容應為轉換後最終欄位值，而非原始欄位值

## 9. ZIPCODE 轉換規則

### 9.1 ZIPCODE 為 null

- 若 `ZIPCODE` 為 SQL `null` 或 `NULL`，則：
  - 抓取ADDRESS3第一至第三字元與zipcode.json city比對和第四至第六字元與zipcode.json name比對：
  - TRUE:
    - COPY city of zipcode.json to `ADDRESS_1`
    - COPY name of zipcode.json to `ADDRESS_2`
    - COPY zipCode of zipcode.json to `ZIPCODE`
    - COPY `null` to `ZIPCODE_ORGIN`
  - FALSE:
    - copy '' to  `ZIPCODE`
    - `ADDRESS_1` 設為空字串 `''`
    - `ADDRESS_2` 設為空字串 `''`
    - COPY `null` to `ZIPCODE_ORGIN`
  - COPY `ADDRESS3` to `ADDRESS_3_ORGIN`
  - CSV COMENTS欄位新增文字"客戶資料無zipcode"

### 9.2 ZIPCODE 不為 null

- 若 `ZIPCODE` 不為 `null`，需以其值對照 `zipcode.json` 中的 `zipCode`
- 若成功找到對應資料：
  - 將 `city` 複製到 `ADDRESS_1`
  - 將 `name` 複製到 `ADDRESS_2`
      

### 9.3 ZIPCODE 查無對應

- 若 `ZIPCODE` 不為 `null`，但在 `zipcode.json` 中找不到對應：
  - 保留 `ZIPCODE` 原值
  - `ADDRESS_1` 設為空字串 `''`
  - `ADDRESS_2` 設為空字串 `''`
  - 該筆需寫入 log，但仍視為可輸出資料
  - CSV COMENTS欄位新增文字"客戶zipcode資料不存在zipcode.json"

## 10. GENDER 轉換規則

- `GENDER` 比對採大小寫不敏感
- 若 `GENDER` 為 `M` 或 `m`，轉為 `'1'`
- `F`、空字串、`null`、其他非預期值，皆轉為 `'0'`
- 非上述2個字元轉為`'9'`並CSV COMENTS欄位新增文字"客戶性別資料不正確"

## 11. null 與值判定規則

- SQL 中的 `null` 與 `NULL` 視為空值
- 字串 `'null'` 不視為 SQL 空值，而是一般字串
- 空字串 `''` 不等於 SQL `null`
- 判斷 `ZIPCODE` 是否為空值時，應依 SQL 值型態判斷，不可單純以文字包含 `null` 判斷

## 12. 錯誤處理

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

## 13. Log 記錄內容

每筆 log 至少應包含：

- 發生時間
- 錯誤等級，可至少區分 `ERROR` 與 `WARN`
- 原始資料所在行號或 SQL 序號
- 原始 SQL 摘要
- 錯誤原因

## 14. 時間規則

- 檔名中的時間使用執行當下本機時間
- `updated_sql_yyyyMMddHHmmss.*` 採 24 小時制
- `log_yyyyMMdd.txt` 使用同一本機日期

## 15. 驗收標準

- 可在未安裝任何第三方套件的 `Python 3.9.10` 環境執行
- 能離線完成轉換
- 能正確產出 SQL、CSV、log 三種檔案
- 遇到錯誤資料時不會整體中止
- `ZIPCODE` 與 `GENDER` 規則符合本規格
- 輸出檔編碼符合需求

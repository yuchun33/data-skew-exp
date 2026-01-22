驗證 10G+ 資料集 JOIN 小表的執行結果

# 環境需求

1. K8S cluster
2. 於 K8S cluster 部署 Kubeflow Spark Operator
3. 於 K8S cluster 部署 minio

# 執行方法

1. 把 spark 檔案上傳到 minio
2. 使用資料夾內的對應 yaml 觸發 spark job

# 驗證結果

## 方法 1：pandas

與 spark 設定同樣的 memory 限制條件，出現 OOMKilled
![pandas-oomkilled.png](/img/pandas-oomkilled.png)

## 方法 2：spark

spark 預設的 autobroadcast 表現最好
![spark-autobroadcast-01.png](/img/spark-autobroadcast-01.png)

## 方法 3：spark 關掉 autobroadcast

memory 不足，無法完成任務
![spark-turnoff-autobroadcast-01.png](/img/spark-turnoff-autobroadcast-01.png)
![spark-turnoff-autobroadcast-02.png](/img/spark-turnoff-autobroadcast-02.png)

## 方法 4：手動 salting

加入 salting 可以讓數據在有限 memory 中「排隊」分批處理，完成任務
![salting-01.png](/img/salting-01.png)
任務被平均分配到 executor 執行，作業時間平均，但是出現 Memory & Disk Spill
![salting-02.png](/img/salting-02.png)
如果拆成更多 partition，可以降低 Memory & Disk Spill，比如切成 400
![salting-03.png](/img/salting-03.png)
增加 memory 到 5G，沒有 Memory & Disk Spill
![salting-04.png](/img/salting-04.png)
但實際作業時間變長
![salting-05.png](/img/salting-05.png)

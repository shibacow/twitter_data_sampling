# twitter_data_sampling

twitterのSampled stream からBigQueryに対してデータを収集するサンプルプログラム。
[こちら](https://qiita.com/shibacow/items/19bfb191f7fee1fba4db) に詳細があります。

# 構成ファイル

- [fluentd.conf](fluentd.conf) twitterの[Sampled stream](https://developer.twitter.com/en/docs/twitter-api/tweets/sampled-stream/introduction) を受けて、GCPのPUBSUBにデータを格納します。
- [strem_count.py](strem_count.py) pubsubからデータを取得し、 整形、BigQueryへの保存まで行う








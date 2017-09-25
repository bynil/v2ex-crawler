# v2ex-crawler
A simple single-threaded crawler for V2EX

# 环境
Python 3.6+

pip install -r requirements.txt

MongoDB

Elasticsearch + IK Analysis plugin

ss-local

# 说明
这是一个简单的、为 [sov2ex](https://github.com/Bynil/sov2ex) 定制的单线程爬虫。开源出来满足大家的好奇心，仅供参考，不具有复用价值。

# 使用
在 config.py 中填入或修改必要的配置信息，shadowsocks 代理是为了提供更多的 IP 资源，可以留空。

如果你没有 Elasticsearch 环境，在 main.py 中注释掉 `fetcher.sync_topic_to_es()`，如果你有 Elasticsearch, 先跑一次 migrate.py 把索引的映射建好，这个工具主要用于将 MongoDB 里已有的数据完全迁移到新的 Elasticsearch 中。

MongoDB 是必须的，否则你就是在为难我胖虎。数据库用户名和密码不是必须的。爬虫会自动在 MongoDB 中建立它需要的索引。

如果你不需要爬取主题的 附言、浏览、感谢、收藏 这些 API 接口中没有的信息，config.py 中的 `V2EX_PASSWORD` 可以留空。但 `V2EX_USERNAME` 是必须的。

注意控制爬虫的请求频率，高频率请求会对 V2EX 的服务器带来压力，这不礼貌，同时也可能使你的 IP 被短时间屏蔽。代码里默认已经使用令牌桶在接口层对此进行限制，令牌桶直接 copy 了 [pyspider](https://github.com/binux/pyspider/blob/master/pyspider/scheduler/token_bucket.py) 的实现。

爬虫是优先爬完所有主题的信息，再开始爬回复的，所以如果你是直接运行，很长时间内你都看不到 reply 的数据。

# 致谢
[pyspider](https://github.com/binux/pyspider)



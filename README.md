# 数据表

|name|affiliation|homepage|scholarid|
|----|----|-----|-----|
|A Min Tjoa	|TU Wien|http://www.ifs.tuwien.ac.at/tjoa |x8qCMhcAAAAJ|

# Api
## google scholor
**有免费套餐，但是不多**

输入：作者名

输出：所有的论文标题、部分摘要、发表信息。

## Aminor
## 根据名字获取期刊、作者、论文id
**免费**

输入：名字

输出：id

### 学者论文
**1.5一次**

输入：学者id

输出：他的所有论文id和标题

### 论文详情
**0.01一次**

输入：论文id

输出：摘要、关键字等，**无正文**

### 论文批量查询

**0.1一次**

输入：关键词数组

输出：具有这些关键词的论文信息


### 根据条件获取论文详情

**0.2一次**

输入：期刊id和年份

输出：所有论文的摘要、关键词、作者等信息

### 混合搜索

**0.05一次**

输入：主题查询、作者查询、组织查询均可

输出：论文id

单用作者名查询非常容易搜不到，Kaiming He等人都搜不到,但是用1.5的那个接口就能获取到


# 代码
封装了几个工具类执行所有操作, 这样能尽可能避免重复的无意义的api调用，而且比较安全（大概吧

| 命令                                 | 功能                           | 花费                |
|------------------------------------|------------------------------|-------------------|
| batch_save_papers(author_name, org) | 获取author_name的所有论文的摘要、关键词等信息 | 1.5 + 0.01 * 论文篇数 |
| analyze_authors(authors_list)      | 自动分析所有作者的论文                  |

# TODO
1. 阅读csranking的数据生成、加载代码，做到根据地区、领域、学校筛选学者姓名
2. csranking的有些名字aminor是搜不到的，需要特殊处理
3. csranking的有些名字有奇怪的结构，比如csranking里是Yi Yang 0001，这个aminor里搜出来的另一个人，搜Yi Yang才是他。
4. 搜Manmohan Krishna Chandraker最匹配的是M. R. Chandrakar，搜Nuno Vasconcelos最匹配的是Nuno M. Vasconcelos。说明有的外国人aminor也没有，有的存的又是其他写法。可能需要让大模型结合其他信息推理是不是同一个人？
5. 重名的情况需要处理。
6. 暂时没有办法在获取论文的详细信息前得到论文的发表时间，没法排除掉一些论文。
7. 要是能定制api的话，输入作者名、机构名等学者信息，输出这个人的全部论文的详情，如果有正文更好。
8. 能不能获取到aminor排行榜上的学者id
9. 如果给作者名加入了不适当的组织信息也会导致搜不出来，比如{"name": "Nuno Vasconcelos", "org": "Univ. of California - San Diego"}无效，{"name": "Nuno Vasconcelos"}有效。但是这个组织名是csranking上的。

# 论文数据爬取

## 目标

爬取一本期刊的所有论文数据, 保存成json文件。

## 步骤

1. 期刊url：https://ieeexplore.ieee.org/xpl/RecentIssue.jsp?punumber=34，issue的url：https://ieeexplore.ieee.org/xpl/issues?punumber=34&isnumber=11192800获取到所有issues的链接
2. 比如其中一个issues是：https://ieeexplore.ieee.org/xpl/tocresult.jsp?isnumber=11192800&punumber=34， 另一个issues是：https://ieeexplore.ieee.org/xpl/tocresult.jsp?isnumber=10269680&punumber=34，区别就是isnumber变了，那isnumber代表着issue的id，punumber代表着期刊id
3. 在issue界面获取到所有论文url，比如https://ieeexplore.ieee.org/document/10269654/。
4. 在论文界面获取到论文的所有详细信息。

那我们需要

首先对于punumber是34的这一本期刊，构造出url，找到他的所有isnumber是多少。进一步构造出url，找到改期所有论文的id是多少，进一步访问论文界面获取到所有信息
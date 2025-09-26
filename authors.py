import pandas as pd
import random
import gzip
from lxml import etree as ElementTree
import re
from collections import defaultdict


class CSRankingsAuthorFilter:
    def __init__(self, data_dir="."):
        """初始化筛选器，加载必要的数据文件"""
        self.data_dir = data_dir

        # 解析DBLP数据并生成必要信息
        self._parse_dblp()

        # 加载所有数据为DataFrame
        self.df_csrankings = self._load_csrankings()
        self.df_aliases = self._load_aliases()
        self.df_country_info = self._load_country_info()
        self.df_countries = self._load_countries()
        self.df_author_areas = self._load_author_areas()

        # 构建辅助映射表
        self._build_mappings()

    def _parse_dblp(self):
        """解析dblp.xml或dblp.xml.gz文件，提取作者-领域关联信息"""
        self.author_publications = defaultdict(list)
        self.conf_areas = {}

        # 从csrankings.js中提取会议与领域的映射关系
        self._initialize_conf_areas()

        # 尝试打开压缩或未压缩的DBLP文件
        dblp_files = [
            f"{self.data_dir}/dblp.xml.gz",
            f"{self.data_dir}/dblp.xml",
            f"{self.data_dir}/dblp-original.xml.gz",
            f"{self.data_dir}/dblp-original.xml"
        ]

        dblp_file = None
        for file in dblp_files:
            try:
                if file.endswith('.gz'):
                    dblp_file = gzip.open(file, 'rb')
                else:
                    dblp_file = open(file, 'rb')
                break
            except FileNotFoundError:
                continue

        if not dblp_file:
            raise FileNotFoundError("未找到DBLP文件，请确保dblp.xml或dblp.xml.gz在数据目录中")

        # 解析DBLP文件
        old_node = None
        for event, node in ElementTree.iterparse(dblp_file, events=["start", "end"]):
            if old_node is not None:
                old_node.clear()
            old_node = node

            if node.tag in ["inproceedings", "article"]:
                conf_name = None
                year = None
                authors = []

                for child in node:
                    if child.tag in ["booktitle", "journal"] and child.text:
                        conf_name = child.text.strip()
                    if child.tag == "year" and child.text:
                        year = child.text.strip()
                    if child.tag == "author" and child.text:
                        authors.append(child.text.strip())

                if conf_name and year and authors:
                    # 确定会议所属领域
                    area = self._get_area_for_conf(conf_name)
                    if area:
                        for author in authors:
                            self.author_publications[author].append({
                                "conf": conf_name,
                                "year": year,
                                "area": area
                            })

        dblp_file.close()

    def _initialize_conf_areas(self):
        """初始化会议与领域的映射关系，基于csrankings.js中的定义"""
        # 从csrankings.js提取的会议-领域映射
        self.conf_areas = {
            "AAAI": "ai",
            "IJCAI": "ai",
            "CVPR": "vision",
            "ECCV": "vision",
            "ICCV": "vision",
            "ICML": "mlmining",
            "KDD": "mlmining",
            "ICLR": "mlmining",
            "NeurIPS": "mlmining",
            "NIPS": "mlmining",
            "ACL": "nlp",
            "EMNLP": "nlp",
            "NAACL": "nlp",
            "SIGIR": "inforet",
            "WWW": "inforet",
            "ASPLOS": "arch",
            "ISCA": "arch",
            "MICRO": "arch",
            "HPCA": "arch",
            "SIGCOMM": "comm",
            "NSDI": "comm",
            "CCS": "sec",
            "Oakland": "sec",
            "USENIX Security": "sec",
            "NDSS": "sec",
            "PETS": "sec",
            "SIGMOD": "mod",
            "VLDB": "mod",
            "ICDE": "mod",
            "PODS": "mod",
            "SC": "hpc",
            "HPDC": "hpc",
            "ICS": "hpc",
            "MobiCom": "mobile",
            "MobiSys": "mobile",
            "SenSys": "mobile",
            "IMC": "metrics",
            "SIGMETRICS": "metrics",
            "SOSP": "ops",
            "OSDI": "ops",
            "FAST": "ops",
            "USENIX ATC": "ops",
            "EuroSys": "ops",
            "PLDI": "plan",
            "POPL": "plan",
            "ICFP": "plan",
            "OOPSLA": "plan",
            "FSE": "soft",
            "ICSE": "soft",
            "ASE": "soft",
            "ISSTA": "soft",
            "FOCS": "act",
            "SODA": "act",
            "STOC": "act",
            "CRYPTO": "crypt",
            "EUROCRYPT": "crypt",
            "CAV": "log",
            "LICS": "log",
            "SIGGRAPH": "graph",
            "SIGGRAPH Asia": "graph",
            "Eurographics": "graph"
        }

    def _get_area_for_conf(self, conf_name):
        """根据会议名称确定所属领域"""
        if not conf_name:
            return None

        # 尝试精确匹配
        for key, area in self.conf_areas.items():
            if key.lower() in conf_name.lower():
                return area

        # 尝试模糊匹配
        conf_lower = conf_name.lower()
        for key, area in self.conf_areas.items():
            if key.lower() in conf_lower:
                return area

        return "unknown"

    def _load_csrankings(self):
        """加载csrankings.csv中的作者基本信息"""
        try:
            df = pd.read_csv(
                f"{self.data_dir}/csrankings.csv",
                usecols=["name", "affiliation", "homepage", "scholarid"],
                dtype=str
            )
        except FileNotFoundError:
            # 如果没有csrankings.csv，从解析的DBLP数据生成一个简化版本
            authors = list(self.author_publications.keys())
            df = pd.DataFrame({
                "name": authors,
                "affiliation": ["unknown"] * len(authors),
                "homepage": ["" for _ in authors],
                "scholarid": ["NOSCHOLARPAGE" for _ in authors]
            })

        # 处理主页结尾斜杠和空值
        df["homepage"] = df["homepage"].str.strip().str.rstrip("/").fillna("")
        df["name"] = df["name"].str.strip()
        df["affiliation"] = df["affiliation"].str.strip().fillna("unknown")
        df["scholarid"] = df["scholarid"].str.strip().fillna("NOSCHOLARPAGE")
        return df.drop_duplicates(subset=["name"]).set_index("name")

    def _load_aliases(self):
        """加载dblp-aliases.csv中的别名映射"""
        try:
            df = pd.read_csv(
                f"{self.data_dir}/dblp-aliases.csv",
                usecols=["name", "alias"],
                dtype=str
            )
        except FileNotFoundError:
            # 如果没有别名文件，创建一个空的DataFrame
            df = pd.DataFrame(columns=["name", "alias"])

        # 清理空值
        df = df.dropna(subset=["name", "alias"])
        df["name"] = df["name"].str.strip()
        df["alias"] = df["alias"].str.strip()
        return df

    def _load_country_info(self):
        """加载机构-国家映射"""
        try:
            df = pd.read_csv(
                f"{self.data_dir}/country-info.csv",
                usecols=["institution", "countryabbrv"],
                dtype=str
            )
        except FileNotFoundError:
            # 如果没有国家信息文件，创建一个空的DataFrame
            df = pd.DataFrame(columns=["institution", "countryabbrv"])

        df = df.dropna(subset=["institution"])
        df["institution"] = df["institution"].str.strip()
        df["countryabbrv"] = df["countryabbrv"].str.strip().fillna("unknown")
        return df.set_index("institution")

    def _load_countries(self):
        """加载国家代码与名称映射"""
        try:
            df = pd.read_csv(
                f"{self.data_dir}/countries.csv",
                usecols=["alpha_2", "name"],
                dtype=str
            )
        except FileNotFoundError:
            # 提供默认的国家代码映射
            df = pd.DataFrame([
                ["us", "United States"],
                ["cn", "China"],
                ["de", "Germany"],
                ["uk", "United Kingdom"],
                ["jp", "Japan"]
            ], columns=["alpha_2", "name"])

        df = df.dropna(subset=["alpha_2", "name"])
        df["alpha_2"] = df["alpha_2"].str.strip().str.lower()
        df["name"] = df["name"].str.strip()
        return df

    def _load_author_areas(self):
        """加载作者研究领域信息"""
        # 从解析的DBLP数据生成作者领域信息
        rows = []
        for author, pubs in self.author_publications.items():
            areas = set()
            for pub in pubs:
                areas.add(pub["area"])
            for area in areas:
                rows.append({"name": author, "area": area})

        # 如果有generated-author-info.csv，使用它补充数据
        try:
            df_file = pd.read_csv(
                f"{self.data_dir}/generated-author-info.csv",
                usecols=["name", "area"],
                dtype=str
            )
            df_file = df_file.dropna(subset=["name", "area"])
            df_file["name"] = df_file["name"].str.strip()
            df_file["area"] = df_file["area"].str.strip()
            rows.extend(df_file.to_dict('records'))
        except FileNotFoundError:
            pass

        df = pd.DataFrame(rows)
        return df

    def _build_mappings(self):
        """构建辅助映射表"""
        # 国家代码<->名称映射
        self.country_code_to_name = dict(zip(
            self.df_countries["alpha_2"],
            self.df_countries["name"]
        ))
        self.country_name_to_code = dict(zip(
            self.df_countries["name"].str.lower(),
            self.df_countries["alpha_2"]
        ))

        # 作者别名映射 (规范名 -> 别名列表)
        self.author_aliases = self.df_aliases.groupby("name")["alias"].agg(list).to_dict()

        # 作者研究领域映射 (作者 -> 领域集合)
        self.author_area_set = self.df_author_areas.groupby("name")["area"].agg(
            lambda x: set(x)).to_dict()

        # 机构->国家代码映射
        self.inst_to_country = self.df_country_info["countryabbrv"].to_dict()

    def get_country_code(self, country_name):
        """根据国家名获取两字母代码(alpha_2)"""
        return self.country_name_to_code.get(country_name.lower())

    def filter_authors(self, countries=None, institutions=None, areas=None,
                       top_k_countries=None, top_k_institutions=None, top_k_areas=None,
                       top_k_authors=None, random_k=None, include_aliases=True):
        """
        筛选作者
        """
        # 初始候选集：所有作者
        authors = self.df_csrankings.index.to_series()
        if authors.empty:
            return []

        # 1. 按国家筛选
        if countries:
            country_codes = []
            for c in countries:
                code = self.get_country_code(c) or (c.lower() if c.lower() in self.country_code_to_name else None)
                if code:
                    country_codes.append(code)

            if country_codes:
                # 获取符合条件的机构
                valid_insts = self.df_country_info[
                    self.df_country_info["countryabbrv"].isin(country_codes)
                ].index.tolist()

                # 筛选出属于这些机构的作者
                authors = authors[
                    self.df_csrankings["affiliation"].isin(valid_insts)
                ]

        # 2. 按机构筛选
        if institutions and not authors.empty:
            authors = authors[
                self.df_csrankings["affiliation"].isin(institutions)
            ]

        # 3. 按研究领域筛选
        if areas and not authors.empty:
            # 找到所有研究领域匹配的作者
            area_mask = self.df_author_areas["area"].isin(areas)
            area_authors = self.df_author_areas[area_mask]["name"].unique()
            authors = authors[authors.isin(area_authors)]

        # 4. 按前k个国家筛选
        if top_k_countries and not authors.empty:
            # 计算每个国家的机构数量并取前k
            country_inst_counts = self.df_country_info[
                "countryabbrv"
            ].value_counts().nlargest(top_k_countries)

            # 筛选这些国家的作者
            valid_insts = self.df_country_info[
                self.df_country_info["countryabbrv"].isin(country_inst_counts.index)
            ].index.tolist()

            authors = authors[
                self.df_csrankings["affiliation"].isin(valid_insts)
            ]

        # 5. 按前k个机构筛选
        if top_k_institutions and not authors.empty:
            # 计算每个机构的作者数量并取前k
            inst_author_counts = self.df_csrankings.loc[authors][
                "affiliation"
            ].value_counts().nlargest(top_k_institutions)

            authors = authors[
                self.df_csrankings["affiliation"].isin(inst_author_counts.index)
            ]

        # 6. 按前k个研究领域筛选
        if top_k_areas and not authors.empty:
            # 计算每个领域的作者数量并取前k
            area_author_counts = self.df_author_areas[
                self.df_author_areas["name"].isin(authors)
            ]["area"].value_counts().nlargest(top_k_areas)

            # 筛选这些领域的作者
            area_mask = self.df_author_areas["area"].isin(area_author_counts.index)
            area_authors = self.df_author_areas[area_mask]["name"].unique()
            authors = authors[authors.isin(area_authors)]

        # 转换为DataFrame便于后续处理
        result_df = self.df_csrankings.loc[authors].reset_index()

        # 7. 取前k个作者（按名称排序）
        if top_k_authors:
            result_df = result_df.sort_values("name").head(top_k_authors)

        # 8. 随机选取k个作者
        if random_k and random_k > 0 and not result_df.empty:
            sample_size = min(random_k, len(result_df))
            result_df = result_df.sample(sample_size)

        # 整理结果，添加别名信息
        final_result = []
        for _, row in result_df.iterrows():
            entry = {
                "name": row["name"],
                "affiliation": row["affiliation"],
                "scholarid": row["scholarid"],
                "areas": list(self.author_area_set.get(row["name"], [])),
                "aliases": self.author_aliases.get(row["name"], []) if include_aliases else []
            }
            final_result.append(entry)

        return final_result

    def get_filtered_countries(self, selected_countries=None):
        """获取符合条件的国家列表"""
        if selected_countries is None:
            return sorted(self.country_code_to_name.values())

        # 只返回已选定的国家（确保存在于数据中）
        valid_countries = []
        for country in selected_countries:
            if country.lower() in self.country_name_to_code:
                valid_countries.append(country)
        return sorted(valid_countries)

    def get_filtered_institutions(self, countries=None):
        """根据选定国家过滤可选学校/机构"""
        if not countries:
            institutions = set(self.df_csrankings["affiliation"].dropna().unique())
            institutions.update(set(self.df_country_info.index.unique()))
            return sorted([inst for inst in institutions if inst])

        # 有国家筛选时，只返回这些国家的机构
        country_codes = []
        for c in countries:
            code = self.get_country_code(c) or (c.lower() if c.lower() in self.country_code_to_name else None)
            if code:
                country_codes.append(code)

        if not country_codes:
            return []

        # 筛选属于目标国家的机构
        valid_insts = self.df_country_info[
            self.df_country_info["countryabbrv"].isin(country_codes)
        ].index.unique()
        # 补充csrankings中可能存在的机构（避免遗漏）
        cs_insts = self.df_csrankings[
            self.df_csrankings["affiliation"].isin(valid_insts)
        ]["affiliation"].unique()
        valid_insts = set(valid_insts) | set(cs_insts)
        return sorted([inst for inst in valid_insts if inst])

    def get_filtered_areas(self, countries=None, institutions=None):
        """根据选定国家/机构过滤可选研究领域"""
        # 先筛选符合条件的作者
        authors = self.df_csrankings.index.to_series()
        if not authors.empty and countries:
            # 按国家筛选作者
            country_codes = [self.get_country_code(c) for c in countries if self.get_country_code(c)]
            valid_insts = self.df_country_info[
                self.df_country_info["countryabbrv"].isin(country_codes)
            ].index.tolist()
            authors = authors[
                authors.isin(self.df_csrankings[self.df_csrankings["affiliation"].isin(valid_insts)].index)]

        if not authors.empty and institutions:
            # 按机构筛选作者
            authors = authors[
                authors.isin(self.df_csrankings[self.df_csrankings["affiliation"].isin(institutions)].index)]

        if authors.empty:
            return []

        # 提取这些作者的研究领域
        areas = self.df_author_areas[self.df_author_areas["name"].isin(authors)]["area"].unique()
        return sorted(areas)


# 使用示例
if __name__ == "__main__":
    # 初始化筛选器，指定数据目录
    filter = CSRankingsAuthorFilter(data_dir='./data')

    # 示例1：随机抽取美国AI领域的5位作者
    us_ai_authors = filter.filter_authors(
        countries=["United States"],
        areas=["ai", "mlmining"],
        random_k=5
    )
    print("美国AI领域的随机5位作者:")
    for author in us_ai_authors:
        print(f"- {author['name']} ({author['affiliation']}), 领域: {', '.join(author['areas'])}")

    # 示例2：获取中国排名前3的机构中的前10位作者，并随机选择3位
    chinese_top_insts_authors = filter.filter_authors(
        countries=["China"],
        top_k_institutions=3,
        top_k_authors=10,
        random_k=3
    )
    print("\n中国排名前3机构中的随机3位作者:")
    for author in chinese_top_insts_authors:
        print(f"- {author['name']} ({author['affiliation']}), 领域: {', '.join(author['areas'])}")

    # 示例3：获取所有数据库领域的作者，并随机选择5位
    db_authors = filter.filter_authors(
        areas=["mod"],
        random_k=5
    )
    print("\n数据库领域的随机5位作者:")
    for author in db_authors:
        print(f"- {author['name']} ({author['affiliation']}), 领域: {', '.join(author['areas'])}")
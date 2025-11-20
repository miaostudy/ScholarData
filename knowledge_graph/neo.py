import json
import re
from neo4j import GraphDatabase, basic_auth
from typing import Dict, List, Any


class Neo4jKnowledgeGraphImporter:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, kg_json_path: str):
        """
        初始化Neo4j导入器
        :param neo4j_uri: Neo4j连接地址（默认：bolt://localhost:7687）
        :param neo4j_user: Neo4j用户名（默认：neo4j）
        :param neo4j_password: Neo4j密码（安装时设置）
        :param kg_json_path: 知识图谱JSON文件路径（即paper_knowledge_graph.json）
        """
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.kg_json_path = kg_json_path

        # 加载知识图谱数据（修复：先加载数据，再初始化entities和relations）
        self.kg_data = self._load_kg_json()
        self.entities = self.kg_data.get("entities", {})
        self.relations = self.kg_data.get("relations", [])

        # 连接Neo4j
        self.driver = self._connect_neo4j()

    def _load_kg_json(self) -> Dict[str, Any]:
        """加载知识图谱JSON文件"""
        try:
            with open(self.kg_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 修复：直接从data中获取关系数量，不访问self.relations
            relation_count = len(data.get("relations", []))
            print(f"✅ 成功加载知识图谱数据：{data['metadata']['paper_count']}篇论文 | {relation_count}条关系")
            return data
        except Exception as e:
            raise ValueError(f"❌ 加载JSON文件失败：{str(e)}")

    def _connect_neo4j(self) -> GraphDatabase.driver:
        """连接Neo4j数据库"""
        try:
            driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=basic_auth(self.neo4j_user, self.neo4j_password),
                max_connection_pool_size=50  # 增大连接池，适配批量导入
            )
            # 测试连接
            driver.verify_connectivity()
            print(f"✅ 成功连接Neo4j：{self.neo4j_uri}")
            return driver
        except Exception as e:
            raise ConnectionError(f"❌ Neo4j连接失败：{str(e)}")

    def _escape_special_chars(self, text: Any) -> Any:
        """转义Cypher语句中的特殊字符（单引号、换行符等）"""
        if not isinstance(text, str):
            return text
        # 转义单引号（Cypher中用两个单引号表示转义）
        text = text.replace("'", "''")
        # 去除换行符和制表符
        text = re.sub(r"[\n\r\t]", " ", text)
        return text.strip()

    def create_indexes(self) -> None:
        """创建节点索引（兼容Neo4j 4.x/5.x，去掉显式索引名）"""
        index_queries = [
            # 为每个实体的唯一ID创建索引（去掉索引名，适配4.x）
            "CREATE INDEX IF NOT EXISTS FOR (k:Keyword) ON (k.node_id)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Paper) ON (p.node_id)",
            "CREATE INDEX IF NOT EXISTS FOR (a:Author) ON (a.node_id)",
            # 为关键词名称创建索引（方便按关键词查询）
            "CREATE INDEX IF NOT EXISTS FOR (k:Keyword) ON (k.name)",
            # 为论文标题创建索引
            "CREATE INDEX IF NOT EXISTS FOR (p:Paper) ON (p.title)"
        ]

        with self.driver.session() as session:
            for query in index_queries:
                session.run(query)
        print("✅ 索引创建完成")

    def import_keyword_nodes(self) -> None:
        """批量导入Keyword节点"""
        keywords = self.entities.get("Keyword", [])
        if not keywords:
            print("⚠️  无Keyword节点可导入")
            return

        # 处理关键词属性（转义特殊字符）
        processed_keywords = [
            {
                "node_id": kw["id"],
                "name": self._escape_special_chars(kw["name"]),
                "weight": kw["weight"],
                "description": self._escape_special_chars(kw["description"])
            }
            for kw in keywords
        ]

        # Cypher批量创建语句（使用MERGE避免重复导入）
        cypher = """
        UNWIND $keywords AS kw
        MERGE (k:Keyword {node_id: kw.node_id})
        SET k.name = kw.name, k.weight = kw.weight, k.description = kw.description
        RETURN count(k) AS created_count
        """

        with self.driver.session() as session:
            result = session.run(cypher, keywords=processed_keywords)
            created_count = result.single()["created_count"]
        print(f"✅ 导入Keyword节点：{created_count}个")

    def import_paper_nodes(self) -> None:
        """批量导入Paper节点"""
        papers = self.entities.get("Paper", [])
        if not papers:
            print("⚠️  无Paper节点可导入")
            return

        processed_papers = [
            {
                "node_id": p["id"],
                "title": self._escape_special_chars(p["title"]),
                "abstract": self._escape_special_chars(p["abstract"]),
                "year": p["year"] if p["year"] else None,  # 处理空年份
                "authors": [self._escape_special_chars(author) for author in p["authors"]]
            }
            for p in papers
        ]

        cypher = """
        UNWIND $papers AS p
        MERGE (pa:Paper {node_id: p.node_id})
        SET pa.title = p.title, pa.abstract = p.abstract, pa.year = p.year, pa.authors = p.authors
        RETURN count(pa) AS created_count
        """

        with self.driver.session() as session:
            result = session.run(cypher, papers=processed_papers)
            created_count = result.single()["created_count"]
        print(f"✅ 导入Paper节点：{created_count}个")

    def import_author_nodes(self) -> None:
        """批量导入Author节点"""
        authors = self.entities.get("Author", [])
        if not authors:
            print("⚠️  无Author节点可导入")
            return

        processed_authors = [
            {
                "node_id": a["id"],
                "name": self._escape_special_chars(a["name"]),
                "paper_count": a["paper_count"],
                "affiliated_papers": a["affiliated_papers"]
            }
            for a in authors
        ]

        cypher = """
        UNWIND $authors AS a
        MERGE (au:Author {node_id: a.node_id})
        SET au.name = a.name, au.paper_count = a.paper_count, au.affiliated_papers = a.affiliated_papers
        RETURN count(au) AS created_count
        """

        with self.driver.session() as session:
            result = session.run(cypher, authors=processed_authors)
            created_count = result.single()["created_count"]
        print(f"✅ 导入Author节点：{created_count}个")

    def import_relations(self) -> None:
        """批量导入关系（分3类处理）"""
        if not self.relations:
            print("⚠️  无关系可导入")
            return

        # 按关系类型分组处理
        relation_groups = {
            "related_to_paper": [],  # 关键词-论文
            "researches_on": [],     # 作者-关键词
            "co_occurrence": []      # 关键词共现
        }

        for rel in self.relations:
            rel_type = rel["relation_type"]
            if rel_type in relation_groups:
                relation_groups[rel_type].append({
                    "source_id": rel["source_id"],
                    "target_id": rel["target_id"],
                    "attributes": self._escape_special_chars(rel["attributes"])
                })

        # 1. 导入关键词-论文关系
        self._import_relation_batch(
            relation_list=relation_groups["related_to_paper"],
            source_label="Keyword",
            target_label="Paper",
            rel_type="RELATED_TO_PAPER",
            rel_attrs=["description"]
        )

        # 2. 导入作者-关键词关系
        self._import_relation_batch(
            relation_list=relation_groups["researches_on"],
            source_label="Author",
            target_label="Keyword",
            rel_type="RESEARCHES_ON",
            rel_attrs=["description"]
        )

        # 3. 导入关键词共现关系
        self._import_relation_batch(
            relation_list=relation_groups["co_occurrence"],
            source_label="Keyword",
            target_label="Keyword",
            rel_type="CO_OCCURRENCE",
            rel_attrs=["count", "description"]
        )

    def _import_relation_batch(
        self,
        relation_list: List[Dict[str, Any]],
        source_label: str,
        target_label: str,
        rel_type: str,
        rel_attrs: List[str]
    ) -> None:
        """批量导入单类型关系"""
        if not relation_list:
            print(f"⚠️  无{rel_type}关系可导入")
            return

        # 构建关系属性赋值语句
        attr_set_clause = ", ".join([f"r.{attr} = rel.attributes.{attr}" for attr in rel_attrs])

        cypher = f"""
        UNWIND $relations AS rel
        MATCH (s:{source_label} {{node_id: rel.source_id}})
        MATCH (t:{target_label} {{node_id: rel.target_id}})
        MERGE (s)-[r:{rel_type}]->(t)
        SET {attr_set_clause}
        RETURN count(r) AS created_count
        """

        with self.driver.session() as session:
            result = session.run(cypher, relations=relation_list)
            created_count = result.single()["created_count"]
        print(f"✅ 导入{rel_type}关系：{created_count}条")

    def clear_database(self) -> None:
        """清空数据库（谨慎使用！用于重新导入）"""
        confirm = input("⚠️  确定要清空Neo4j数据库吗？(输入'y'确认)：")
        if confirm.lower() != "y":
            print("❌ 取消清空操作")
            return

        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("✅ 数据库已清空")

    def run_full_import(self, clear_first: bool = False) -> None:
        """执行完整导入流程"""
        try:
            print("\n" + "="*50 + " 开始导入 " + "="*50)
            # 可选：清空数据库
            if clear_first:
                self.clear_database()
            # 1. 创建索引
            self.create_indexes()
            # 2. 导入节点（顺序：Keyword → Paper → Author）
            self.import_keyword_nodes()
            self.import_paper_nodes()
            self.import_author_nodes()
            # 3. 导入关系
            self.import_relations()
            print("\n" + "="*50 + " 导入完成 " + "="*50)
        except Exception as e:
            print(f"\n❌ 导入失败：{str(e)}")
        finally:
            # 关闭连接
            self.driver.close()
            print("✅ Neo4j连接已关闭")


if __name__ == "__main__":
    # -------------------------- 配置参数（请根据你的Neo4j修改）--------------------------
    NEO4J_URI = "bolt://localhost:7687"  # 默认本地连接地址
    NEO4J_USER = "neo4j"                 # 默认用户名
    NEO4J_PASSWORD = "12345678"            # 你安装Neo4j时设置的密码
    KG_JSON_PATH = "paper_knowledge_graph_cache/paper_knowledge_graph.json"  # 你的知识图谱JSON路径
    # ----------------------------------------------------------------------------------

    # 创建导入器实例
    importer = Neo4jKnowledgeGraphImporter(
        neo4j_uri=NEO4J_URI,
        neo4j_user=NEO4J_USER,
        neo4j_password=NEO4J_PASSWORD,
        kg_json_path=KG_JSON_PATH
    )

    # 执行导入（clear_first=True 表示先清空数据库，首次导入建议设为True）
    importer.run_full_import(clear_first=True)
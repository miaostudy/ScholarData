import os
import json
import re
import requests
import hashlib
import threading
from datetime import datetime
from collections import defaultdict, Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = ["SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


class PaperKnowledgeGraphBuilder:
    def __init__(self, papers_dir, glm_api_key, cache_dir="paper_knowledge_graph_cache"):
        self.papers_dir = os.path.abspath(papers_dir)
        self.glm_api_key = glm_api_key
        self.cache_dir = cache_dir

        # 简化缓存文件：只保留必要的缓存，去掉全局缓存key相关
        self.paper_cache = os.path.join(cache_dir, "paper_cache.json")  # 论文数据缓存（key: paper_id）
        self.keywords_cache = os.path.join(cache_dir, "keywords_cache.json")  # 关键词缓存
        self.relations_cache = os.path.join(cache_dir, "relations_cache.json")  # 关系缓存

        # 初始化缓存目录和文件
        self._init_cache()

        # 加载缓存（直接用论文ID作为key，避免重复）
        self.paper_data = self._load_cache(self.paper_cache)  # 格式：{paper_id: paper_info}
        self.keywords_data = self._load_cache(self.keywords_cache)
        self.relations_data = self._load_cache(self.relations_cache)

        # 简化线程锁
        self.cache_locks = {
            "paper": threading.Lock(),
            "keywords": threading.Lock(),
            "relations": threading.Lock()
        }

    def _init_cache(self):
        """初始化缓存目录和空缓存文件"""
        os.makedirs(self.cache_dir, exist_ok=True)
        for cache_file in [self.paper_cache, self.keywords_cache, self.relations_cache]:
            if not os.path.exists(cache_file):
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)

    def _load_cache(self, file_path):
        """加载缓存文件，异常时返回空字典"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载缓存 {os.path.basename(file_path)} 失败：{str(e)}，使用空字典")
            return {}

    def _save_cache(self, data, file_path, lock_name):
        """实时保存缓存（单篇论文处理完立即保存）"""
        lock = self.cache_locks.get(lock_name)
        if not lock:
            print(f"无效的锁名称：{lock_name}")
            return False
        with lock:
            try:
                temp_path = f"{file_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, file_path)  # 原子操作
                return True
            except Exception as e:
                print(f"保存缓存 {os.path.basename(file_path)} 失败：{str(e)}")
                return False

    def load_paper_data(self, force_refresh=False):
        """加载论文数据：遍历JSON文件，处理一篇缓存一篇，避免重复"""
        print(f"正在读取论文文件夹：{self.papers_dir}")

        # 获取所有JSON文件
        all_json_files = [f for f in os.listdir(self.papers_dir) if f.endswith(".json")]
        total_files = len(all_json_files)
        print(f"发现 {total_files} 篇论文文件")

        processed_count = 0
        valid_count = 0
        invalid_count = 0

        for idx, filename in enumerate(all_json_files, 1):
            file_path = os.path.join(self.papers_dir, filename)
            paper_id = filename.replace(".json", "")

            # 跳过已处理的论文（除非强制刷新）
            if not force_refresh and paper_id in self.paper_data:
                print(f"[{idx}/{total_files}] 论文 {paper_id} 已缓存，跳过")
                processed_count += 1
                valid_count += 1
                continue

            try:
                # 读取单篇论文
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # 提取核心字段
                paper_info = {
                    "paper_id": paper_id,
                    "title": data.get("title", "Unknown Title"),
                    "abstract": data.get("abstract", "").strip(),
                    "authors": data.get("authors", []),
                    "publication_date": data.get("publication_date", ""),
                    "year": self._parse_year(data.get("publication_date", "")),
                    "ieee_keywords": self._standardize_keywords(data.get("ieee_keywords", [])),
                    "index_terms": self._standardize_keywords(data.get("index_terms", [])),
                    "author_keywords": self._standardize_keywords(data.get("author_keywords", [])),
                }
                # 合并所有关键词
                paper_info["all_keywords"] = list(set(
                    paper_info["ieee_keywords"] + paper_info["index_terms"] + paper_info["author_keywords"]
                ))

                # 保存到缓存（处理一篇保存一篇）
                self.paper_data[paper_id] = paper_info
                self._save_cache(self.paper_data, self.paper_cache, "paper")

                print(f"[{idx}/{total_files}] 成功读取论文 {paper_id}（标题：{paper_info['title'][:50]}...）")
                valid_count += 1
                processed_count += 1

            except Exception as e:
                print(f"[{idx}/{total_files}] 读取论文 {paper_id} 失败：{str(e)}")
                invalid_count += 1

        # 转换为列表格式返回（便于后续处理）
        paper_list = list(self.paper_data.values())
        print(f"\n论文数据读取完成：有效论文 {valid_count} 篇，无效论文 {invalid_count} 篇，已缓存 {len(paper_list)} 篇")
        return paper_list

    def _standardize_keywords(self, keywords):
        """关键词标准化"""
        if not isinstance(keywords, list):
            return []
        standardized = []
        for kw in keywords:
            if isinstance(kw, str) and kw.strip():
                standardized_kw = kw.strip().lower()
                if standardized_kw not in standardized:
                    standardized.append(standardized_kw)
        return standardized

    def _parse_year(self, date_str):
        """解析年份"""
        if not date_str or not isinstance(date_str, str):
            return None
        match = re.search(r"\b(19|20)\d{2}\b", date_str)
        return int(match.group()) if match else None

    def _extract_keywords_from_paper(self, paper):
        """单篇论文关键词提取（修复空响应解析错误）"""
        paper_id = paper["paper_id"]
        title = paper["title"]
        abstract = paper["abstract"]
        existing_kw = paper["all_keywords"]  # 已有的关键词（仅API调用失败时使用）

        # 1. 如果没有摘要，直接返回已有关键词
        if not abstract:
            print(f"论文 {paper_id} 无摘要，返回已有关键词")
            return existing_kw

        # 2. 提示词：明确优先输出JSON，避免思考过程过长
        prompt = f"""
        Task: Extract core academic keywords from the title and abstract (if any).
        Research Field: All academic fields (adapt to the paper's actual field automatically)

        Keyword Requirements:
        1. Accuracy: Strictly based on the content, no irrelevant keywords
        2. Conciseness: Use single terms or short phrases (2-4 words), avoid long sentences
        3. Consistency: Use standard academic terminology, prefer well-known abbreviations (e.g., "nlp" instead of "natural language processing")
        4. Quantity: No limit (can be 0 if no valid keywords can be extracted)

        Output Format:
        ONLY return JSON, no extra text, no comments, no reasoning.
        Example (with keywords): {{"keywords": ["transformer", "natural language processing"]}}
        Example (no keywords): {{"keywords": []}}

        Paper Content:
        Title: {title}
        Abstract: {abstract[:1000]}  # 截断过长摘要，避免提示词占用过多Token
        """

        # 3. 系统提示词：强调只输出JSON，不额外内容
        system_msg = """
        You are a professional academic keyword extraction expert.
        Extract keywords strictly based on the paper content.
        ONLY output JSON format as required, no extra reasoning or explanation.
        If no valid keywords can be found, return {{"keywords": []}}.
        """

        # 4. API调用：仅异常时重试
        response = None
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                response = self._call_glm_api(prompt, system_message=system_msg.strip())
                break
            except Exception as e:
                retry_count += 1
                print(f"论文 {paper_id} 关键词提取重试 {retry_count}/{max_retries}：{str(e)}")

        # 5. 处理API调用失败
        if response is None:
            print(f"论文 {paper_id} 大模型调用失败，返回已有关键词")
            return existing_kw

        # 6. 修复空响应解析错误（核心修改）
        try:
            # 先判断响应是否为空
            response = response.strip().strip("`").strip()
            if not response:  # 响应为空字符串
                print(f"论文 {paper_id} 大模型返回空响应，返回空关键词列表")
                return []  # 按需求返回空列表，也可改为 return existing_kw
            if response.startswith("json"):
                response = response[4:].strip()
            result = json.loads(response)
            extracted_kw = self._standardize_keywords(result.get("keywords", []))
            print(f"论文 {paper_id} 提取关键词 {len(extracted_kw)} 个：{extracted_kw[:5]}...")
            return extracted_kw
        except Exception as e:
            print(f"论文 {paper_id} 解析关键词失败：{str(e)}，返回已有关键词")
            return existing_kw

    def extract_and_merge_keywords(self, paper_list, force_refresh=False):
        """提取+合并关键词"""
        cache_key = "all_keywords"
        if not force_refresh and cache_key in self.keywords_data:
            print("从缓存加载关键词数据")
            return self.keywords_data[cache_key]

        print(f"开始处理关键词（共 {len(paper_list)} 篇论文）")
        keywords_counter = Counter()

        # 初始化单篇论文关键词缓存
        if "paper_keywords" not in self.keywords_data:
            self.keywords_data["paper_keywords"] = {}

        for idx, paper in enumerate(paper_list, 1):
            paper_id = paper["paper_id"]
            title = paper["title"]

            # 优先使用已缓存的提取关键词
            if not force_refresh and paper_id in self.keywords_data["paper_keywords"]:
                extracted_kw = self.keywords_data["paper_keywords"][paper_id]
                print(f"[{idx}/{len(paper_list)}] 论文 {paper_id} 已缓存关键词（{len(extracted_kw)}个），跳过")
            else:
                print(f"[{idx}/{len(paper_list)}] 正在提取论文 {paper_id} 关键词（标题：{title[:50]}...）")
                extracted_kw = self._extract_keywords_from_paper(paper)

                # 缓存单篇论文的提取关键词
                self.keywords_data["paper_keywords"][paper_id] = extracted_kw
                self._save_cache(self.keywords_data, self.keywords_cache, "keywords")

            # 统计关键词（去重）
            for kw in set(extracted_kw):
                keywords_counter[kw] += 1

            # 进度提示
            if idx % 10 == 0 or idx == len(paper_list):
                print(f"[{idx}/{len(paper_list)}] 已处理 {idx} 篇论文，累计提取关键词类型 {len(keywords_counter)} 个")

        # 合并相似关键词
        merged_keywords = self._merge_similar_keywords(dict(keywords_counter))

        # 缓存最终关键词
        self.keywords_data[cache_key] = merged_keywords
        self._save_cache(self.keywords_data, self.keywords_cache, "keywords")

        print(f"\n关键词处理完成：共 {len(merged_keywords)} 个核心关键词（Top10：{list(merged_keywords.keys())[:10]}）")
        return merged_keywords

    def _merge_similar_keywords(self, keywords):
        """合并相似关键词"""
        if len(keywords) < 2:
            return keywords

        prompt = f"""
        Task: Merge semantically similar academic keywords, retain the most representative term, sum their weights.
        Research Field: All academic fields (adapt to the keywords' actual field)

        Requirements:
        1. Merge only highly similar keywords (e.g., "nlp" ↔ "natural language processing", "cnn" ↔ "convolutional neural network")
        2. Do NOT merge unrelated keywords (e.g., "image processing" ↔ "text classification" are not merged)
        3. Representative term: Prefer concise, well-known terms or abbreviations (e.g., use "nlp" instead of "natural language processing")
        4. Weight calculation: Sum the weights of all merged keywords (do not change the total weight)
        5. Completeness: Do not delete any keywords (all input keywords must be merged into some representative term)
        6. Output format: Strict JSON, no extra text. Example:
           {{"merged_keywords": [{{"word": "nlp", "weight": 15}}, {{"word": "cnn", "weight": 12}}]}}

        Input Keywords (word: weight):
        """
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        for word, weight in sorted_keywords[:50]:
            prompt += f"- {word}: {weight}\n"

        if len(sorted_keywords) > 50:
            extra_kw = [word for word, _ in sorted_keywords[50:]]
            prompt += f"- Additional keywords (low weight): {', '.join(extra_kw[:20])}...\n"

        system_msg = """
        You are a professional academic keyword merging expert.
        Your task is to merge semantically similar keywords while maintaining the core meaning and weight.
        Ensure that the merged keywords are representative and widely used in academic circles.
        If you are unsure whether two keywords are similar, do NOT merge them.
        """

        response = self._call_glm_api(prompt, system_message=system_msg.strip())
        if not response:
            print("关键词合并失败，返回原始关键词")
            return keywords

        try:
            response = response.strip().strip("`").strip()
            if response.startswith("json"):
                response = response[4:].strip()
            result = json.loads(response)
            merged = {}
            for item in result.get("merged_keywords", []):
                word = item.get("word", "").strip().lower()
                weight = item.get("weight", 0)
                if word and weight > 0:
                    merged[word] = weight

            if len(merged) < max(5, len(keywords) // 3):
                print("关键词合并结果异常，返回原始关键词")
                return keywords

            return dict(sorted(merged.items(), key=lambda x: x[1], reverse=True))
        except Exception as e:
            print(f"解析合并关键词结果失败：{str(e)}，返回原始关键词")
            return keywords

    def _call_glm_api(self, prompt, system_message=None):
        """调用GLM API（调整max_tokens避免截断）"""
        api_url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.glm_api_key}",
            "Content-Type": "application/json"
        }

        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt.strip()})

        payload = {
            "model": "glm-4.5v",
            "messages": messages,
            "temperature": 0.3,
            "response_format": {"type": "json_object"},
            "timeout": 300,
            "max_tokens": 768
        }

        try:
            response = requests.post(
                api_url,
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False),
                timeout=300
            )
            response.raise_for_status()
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            print("API返回结果为空")
            return ""
        except Exception as e:
            print(f"API调用异常：{str(e)}")
            raise

    def build_entities_and_relations(self, paper_list, merged_keywords, force_refresh=False):
        """构建实体和关系"""
        if not force_refresh and "all_entities" in self.relations_data and "all_relations" in self.relations_data:
            print("从缓存加载实体和关系数据")
            return self.relations_data["all_entities"], self.relations_data["all_relations"]

        print("开始构建实体和关系...")

        entities = {
            "Keyword": [],
            "Paper": [],
            "Author": []
        }

        for word, weight in merged_keywords.items():
            entities["Keyword"].append({
                "id": f"kw_{hashlib.md5(word.encode()).hexdigest()[:8]}",
                "name": word,
                "weight": weight,
                "description": f"Core research concept (frequency: {weight})"
            })

        for paper in paper_list:
            entities["Paper"].append({
                "id": f"paper_{hashlib.md5(paper['paper_id'].encode()).hexdigest()[:8]}",
                "title": paper["title"],
                "abstract": paper["abstract"][:200] + "..." if len(paper["abstract"]) > 200 else paper["abstract"],
                "year": paper["year"],
                "authors": [a.strip() for a in paper["authors"] if a.strip()]
            })

        # 作者实体（去重）
        author_papers = defaultdict(list)
        paper_id_map = {p["title"]: p["id"] for p in entities["Paper"]}
        for paper in paper_list:
            paper_id = paper_id_map.get(paper["title"])
            if not paper_id:
                continue
            for author in paper["authors"]:
                author = author.strip()
                if author:
                    author_papers[author].append(paper_id)

        for author, papers in author_papers.items():
            entities["Author"].append({
                "id": f"author_{hashlib.md5(author.encode()).hexdigest()[:8]}",
                "name": author,
                "paper_count": len(papers),
                "affiliated_papers": papers
            })

        # 构建关系
        relations = []
        kw_name_to_id = {kw["name"]: kw["id"] for kw in entities["Keyword"]}
        paper_title_to_id = {p["title"]: p["id"] for p in entities["Paper"]}
        author_name_to_id = {a["name"]: a["id"] for a in entities["Author"]}

        # 关键词-论文关联
        for paper in paper_list:
            paper_id = paper_title_to_id.get(paper["title"])
            if not paper_id:
                continue
            extracted_kw = self.keywords_data["paper_keywords"].get(paper["paper_id"], [])
            for kw in extracted_kw:
                kw_id = kw_name_to_id.get(kw)
                if kw_id:
                    relations.append({
                        "source_id": kw_id,
                        "target_id": paper_id,
                        "relation_type": "related_to_paper",
                        "attributes": {"description": f"Keyword '{kw}' related to paper"}
                    })

        # 作者-关键词关联
        for paper in paper_list:
            paper_id = paper_title_to_id.get(paper["title"])
            if not paper_id:
                continue
            extracted_kw = self.keywords_data["paper_keywords"].get(paper["paper_id"], [])
            paper_kw = [kw for kw in extracted_kw if kw in kw_name_to_id]
            for author in paper["authors"]:
                author = author.strip()
                author_id = author_name_to_id.get(author)
                if author_id:
                    for kw_id in [kw_name_to_id[kw] for kw in paper_kw]:
                        relations.append({
                            "source_id": author_id,
                            "target_id": kw_id,
                            "relation_type": "researches_on",
                            "attributes": {"description": f"Author researches on keyword '{kw}'"}
                        })

        # 关键词共现关系
        co_occur_counter = defaultdict(int)
        for paper in paper_list:
            extracted_kw = self.keywords_data["paper_keywords"].get(paper["paper_id"], [])
            paper_kw = [kw for kw in extracted_kw if kw in kw_name_to_id]
            for i in range(len(paper_kw)):
                for j in range(i + 1, len(paper_kw)):
                    kw1, kw2 = paper_kw[i], paper_kw[j]
                    pair_key = tuple(sorted([kw1, kw2]))
                    co_occur_counter[pair_key] += 1

        for (kw1, kw2), count in co_occur_counter.items():
            if count >= 2:
                relations.append({
                    "source_id": kw_name_to_id[kw1],
                    "target_id": kw_name_to_id[kw2],
                    "relation_type": "co_occurrence",
                    "attributes": {"count": count, "description": f"Co-occur {count} times in papers"}
                })

        # 缓存实体和关系
        self.relations_data["all_entities"] = entities
        self.relations_data["all_relations"] = relations
        self._save_cache(self.relations_data, self.relations_cache, "relations")

        print(f"实体和关系构建完成：")
        print(
            f" - 实体：关键词 {len(entities['Keyword'])} 个 | 论文 {len(entities['Paper'])} 篇 | 作者 {len(entities['Author'])} 位")
        print(f" - 关系：共 {len(relations)} 条")
        return entities, relations

    def generate_wordcloud(self, keywords, title, output_filename):
        """生成词云图"""
        if not keywords:
            print("没有关键词可生成词云")
            return

        filtered_kw = {kw: weight for kw, weight in keywords.items() if weight >= 2}
        if not filtered_kw:
            print("没有足够权重的关键词（需≥2）")
            return

        wordcloud = WordCloud(
            width=1200, height=800,
            background_color="white",
            max_words=50,
            contour_width=2,
            contour_color="steelblue",
            random_state=42
        ).generate_from_frequencies(filtered_kw)

        plt.figure(figsize=(15, 10))
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")
        plt.title(title, fontsize=20, pad=20, fontweight="bold")

        output_path = os.path.join(self.cache_dir, output_filename)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"词云图已保存至：{output_path}")

    def build_knowledge_graph(self, force_refresh=False):
        """构建知识图谱"""
        print("=" * 80)
        print("开始构建论文知识图谱")
        print("=" * 80)

        # 步骤1：加载论文数据
        paper_list = self.load_paper_data(force_refresh=force_refresh)
        if not paper_list:
            print("没有有效论文数据，终止构建")
            return None

        # 步骤2：提取并合并关键词
        merged_keywords = self.extract_and_merge_keywords(paper_list, force_refresh=force_refresh)
        self.generate_wordcloud(merged_keywords, "核心关键词词云", "keywords_wordcloud.png")

        # 步骤3：构建实体和关系
        entities, relations = self.build_entities_and_relations(paper_list, merged_keywords,
                                                                force_refresh=force_refresh)

        # 构建最终知识图谱
        knowledge_graph = {
            "metadata": {
                "build_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "paper_count": len(paper_list),
                "entity_count": {
                    "Keyword": len(entities["Keyword"]),
                    "Paper": len(entities["Paper"]),
                    "Author": len(entities["Author"])
                },
                "relation_count": len(relations),
                "source_folder": self.papers_dir
            },
            "entities": entities,
            "relations": relations
        }

        # 保存最终结果
        kg_path = os.path.join(self.cache_dir, "paper_knowledge_graph.json")
        with open(kg_path, "w", encoding="utf-8") as f:
            json.dump(knowledge_graph, f, ensure_ascii=False, indent=2)
        print(f"\n知识图谱已保存至：{kg_path}")

        # 输出摘要
        print("\n" + "=" * 80)
        print("知识图谱构建完成！")
        print(f"📊 概览：{len(paper_list)} 篇论文 | {len(entities['Keyword'])} 个关键词 | {len(relations)} 条关系")
        print("=" * 80)

        return knowledge_graph


if __name__ == "__main__":
    # 配置参数
    PAPERS_DIR = "../ieee/json_cache/papers"
    GLM_API_KEY = "38ef8158834549efa2404f4cb748cf73.fO94Wjp0BxJ80a1T"
    CACHE_DIR = "paper_knowledge_graph_cache"
    FORCE_REFRESH = False  # 必须设为True，重新处理之前失败的论文

    # 验证参数
    if not os.path.exists(PAPERS_DIR):
        print(f"错误：论文文件夹 {PAPERS_DIR} 不存在")
        exit(1)
    if not GLM_API_KEY or GLM_API_KEY.startswith("your_"):
        print("错误：请填写有效的GLM API密钥")
        exit(1)

    # 执行构建
    kg_builder = PaperKnowledgeGraphBuilder(PAPERS_DIR, GLM_API_KEY, CACHE_DIR)
    kg_builder.build_knowledge_graph(FORCE_REFRESH)
import os
import json
import time
import re
import requests
from datetime import datetime
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import defaultdict
import threading
from aminor import AMinerController  # 假设工具类在同一目录

plt.rcParams["font.family"] = ["SimHei"]


class AMinerAnalyzer:
    def __init__(self, token, glm_api_key, cache_dir="aminer_cache"):
        self.controller = AMinerController(token, cache_dir)
        self.cache_dir = cache_dir
        self.glm_api_key = glm_api_key

        self.keywords_cache_path = os.path.join(cache_dir, "keywords_cache.json")
        self.merged_keywords_cache_path = os.path.join(cache_dir, "merged_keywords_cache.json")
        self.themes_cache_path = os.path.join(cache_dir, "themes_cache.json")

        self._ensure_analyzer_cache_files()
        self.keywords_cache = self._load_analyzer_cache(self.keywords_cache_path)
        self.merged_keywords_cache = self._load_analyzer_cache(self.merged_keywords_cache_path)
        self.themes_cache = self._load_analyzer_cache(self.themes_cache_path)

        self.keywords_lock = threading.Lock()
        self.merged_lock = threading.Lock()
        self.themes_lock = threading.Lock()

    def _ensure_analyzer_cache_files(self):
        for path in [self.keywords_cache_path, self.merged_keywords_cache_path, self.themes_cache_path]:
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)

    def _load_analyzer_cache(self, file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载分析缓存文件 {file_path} 失败：{str(e)}，使用空字典")
            return {}

    def _save_analyzer_cache(self, data, file_path, lock):
        with lock:
            try:
                temp_path = f"{file_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, file_path)
                return True
            except Exception as e:
                print(f"保存分析缓存文件 {file_path} 失败：{str(e)}")
                return False

    def _extract_year(self, paper_details):
        if not paper_details:
            return None

        year = paper_details.get("year")
        if year:
            return int(year)
        pub_time = paper_details.get("pub_time", "")
        if pub_time:
            match = re.search(r'\b20\d{2}\b', pub_time)
            if match:
                return int(match.group())

        return None

    def _filter_papers_by_year(self, papers, start_year=2015, end_year=2025):
        filtered = []
        for paper in papers:
            paper_id = paper["paper_id"]
            details = self.controller.get_paper_details(paper_id)

            if not details:
                continue

            # 过滤没有摘要的论文
            abstract = details.get("abstract", "").strip()
            if not abstract:
                print(f"论文 {paper_id} 没有摘要，已过滤")
                continue

            year = self._extract_year(details)
            if year and start_year <= year <= end_year:
                filtered.append({
                    "paper_id": paper_id,
                    "title": paper["title"],
                    "year": year,
                    "details": details
                })

        return filtered

    def load_authors_data(self, authors_list, force_refresh=False):
        all_papers = []

        for author_info in authors_list:
            if isinstance(author_info, dict):
                author_name = author_info["name"]
                org = author_info.get("org")
            else:
                author_name = author_info
                org = None

            print(f"\n处理作者: {author_name} {f'({org})' if org else ''}")

            author_papers = self.controller.get_author_papers(author_name, org, force_refresh)

            if not author_papers or "papers_old" not in author_papers:
                print(f"未获取到 {author_name} 的论文数据，跳过")
                continue

            self.controller.batch_save_papers(author_name, org, force_refresh)

            filtered_papers = self._filter_papers_by_year(author_papers["papers_old"])
            print(f"筛选出 {len(filtered_papers)} 篇 {2015}-{2025} 年且有摘要的论文")

            for paper in filtered_papers:
                paper["author_name"] = author_name
                paper["author_org"] = org

            all_papers.extend(filtered_papers)

        return all_papers

    def extract_keywords(self, papers, force_refresh=False):
        cache_key = "_".join(sorted([p["paper_id"] for p in papers]))

        if not force_refresh and cache_key in self.keywords_cache:
            print("从缓存加载关键词数据")
            return self.keywords_cache[cache_key]

        print("开始提取关键词...")
        keywords = defaultdict(int)

        for paper in papers:
            details = paper["details"]
            paper_id = paper["paper_id"]

            # 检查是否有关键词字段
            if "keywords" in details and details["keywords"]:
                for kw in details["keywords"]:
                    keywords[kw.strip().lower()] += 1
            else:
                # 没有关键词字段，使用大模型从标题和摘要提取
                print(f"论文 {paper_id} 没有关键词，使用大模型提取...")

                title = details.get("title", "")
                abstract = details.get("abstract", "")

                # 构建提示词（英文）
                prompt = f"""
                Task: Extract 5-8 keywords that best represent the core content of the papers from the following title and abstract.

                Requirements:
                1. Keywords should accurately reflect the research content and theme of the papers
                2. Avoid overly broad or overly specific terms
                3. Output must be in strict JSON format without any additional explanatory text
                4. JSON structure: {{"keywords": ["keyword1", "keyword2", ...]}}

                Paper title: {title}

                Paper abstract: {abstract}
                """

                # 调用大模型
                response_content = self._call_glm_api(
                    prompt,
                    "You are a professional academic papers analysis tool, specialized in extracting core keywords from papers titles and abstracts. Please respond in English."
                )

                if not response_content:
                    print(f"大模型调用失败，无法为论文 {paper_id} 提取关键词，程序将停止运行")
                    # 保存已提取的关键词到缓存
                    self.keywords_cache[cache_key] = dict(keywords)
                    self._save_analyzer_cache(self.keywords_cache, self.keywords_cache_path, self.keywords_lock)
                    exit(1)

                try:
                    keywords_data = json.loads(response_content)
                    for kw in keywords_data.get("keywords", []):
                        keywords[kw.strip().lower()] += 1
                except:
                    print(f"解析论文 {paper_id} 的关键词提取结果失败，程序将停止运行")
                    # 保存已提取的关键词到缓存
                    self.keywords_cache[cache_key] = dict(keywords)
                    self._save_analyzer_cache(self.keywords_cache, self.keywords_cache_path, self.keywords_lock)
                    exit(1)

        sorted_keywords = dict(sorted(keywords.items(), key=lambda x: x[1], reverse=True))

        self.keywords_cache[cache_key] = sorted_keywords
        self._save_analyzer_cache(self.keywords_cache, self.keywords_cache_path, self.keywords_lock)

        return sorted_keywords

    def _call_glm_api(self, prompt, system_message=None):
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
            "model": "glm-4.5",
            "messages": messages,
            "temperature": 0.3,
            "response_format": {"type": "json_object"}
        }

        try:
            response = requests.post(
                api_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=60
            )
            response.raise_for_status()
            result = response.json()

            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            else:
                print("大模型返回结果格式异常")
                return None

        except Exception as e:
            print(f"大模型API调用失败：{str(e)}")
            return None

    def merge_similar_keywords(self, keywords, force_refresh=False):
        cache_items = sorted(keywords.items())
        cache_key = json.dumps(cache_items)

        if not force_refresh and cache_key in self.merged_keywords_cache:
            print("从缓存加载合并后的关键词数据")
            return self.merged_keywords_cache[cache_key]

        print("调用大模型API合并语义相近的关键词...")

        # 构建英文提示词
        prompt = """
        Task: Merge semantically similar words from the input keyword list, retain the most representative word as the result after merging, and summarize their weights.

        Requirements:
        1. Only merge highly semantically related keywords, avoid over-merging
        2. The merged representative word should accurately cover the core meaning of the merged words
        3. The weight is the sum of the weights of the merged words
        4. Do not change the semantic category and domain characteristics of the original keywords
        5. Words that are obviously unrelated should be kept separately
        6. Output must be in strict JSON format without any additional explanatory text
        7. JSON structure: {"merged_keywords": [{"word": "representative word", "weight": total weight}, ...]}

        Input keyword list (format: word: weight):
        """

        keywords_str = "\n".join([f"{word}: {weight}" for word, weight in keywords.items()])
        prompt += keywords_str

        response_content = self._call_glm_api(
            prompt,
            "You are a professional keyword processing tool, specialized in semantic analysis and word merging and classification. Please respond in English."
        )

        if not response_content:
            print("大模型API调用失败，程序将停止运行")
            exit(1)

        try:
            merged_data = json.loads(response_content)
            merged_result = {
                item["word"]: item["weight"]
                for item in merged_data["merged_keywords"]
            }
            merged_result = dict(sorted(merged_result.items(), key=lambda x: x[1], reverse=True))
        except:
            print("解析大模型结果失败，程序将停止运行")
            exit(1)

        self.merged_keywords_cache[cache_key] = merged_result
        self._save_analyzer_cache(self.merged_keywords_cache, self.merged_keywords_cache_path, self.merged_lock)

        return merged_result

    def extract_themes_from_abstracts(self, papers, force_refresh=False):
        cache_key = "_".join(sorted([p["paper_id"] for p in papers]))

        if not force_refresh and cache_key in self.themes_cache:
            print("从缓存加载主题数据")
            return self.themes_cache[cache_key]

        print("调用大模型API从摘要中提取主题...")

        abstracts = []
        for i, paper in enumerate(papers, 1):
            details = paper["details"]
            abstract = details.get("abstract", "")
            if abstract:
                abstracts.append(f"Abstract {i}: {abstract}")

        if not abstracts:
            print("没有可用的摘要数据，无法提取主题")
            return {}

        # 构建英文提示词
        prompt = """
        Task: Analyze the following collection of research papers abstracts and extract the core research themes and key concepts.

        Requirements:
        1. Extract several core theme words or phrases that best represent these abstracts
        2. Each theme word should be concise and clearly reflect the research content
        3. Assign a weight (1-10) to each theme word, where a higher weight indicates a higher frequency or greater importance of the theme
        4. Avoid duplicate or very semantically similar theme words
        5. Output must be in strict JSON format without any additional explanatory text
        6. JSON structure: {"themes": [{"word": "theme word", "weight": weight}, ...]}

        Abstract collection:
        """
        prompt += "\n\n".join(abstracts[:50])

        response_content = self._call_glm_api(
            prompt,
            "You are a professional literature analysis tool, specialized in extracting core themes and research concepts from academic papers_old. Please respond in English."
        )

        if not response_content:
            print("大模型API调用失败，程序将停止运行")
            exit(1)

        try:
            themes_data = json.loads(response_content)
            themes_result = {
                item["word"]: item["weight"]
                for item in themes_data["themes"]
            }
            themes_result = dict(sorted(themes_result.items(), key=lambda x: x[1], reverse=True))
        except:
            print("解析大模型结果失败，程序将停止运行")
            exit(1)

        self.themes_cache[cache_key] = themes_result
        self._save_analyzer_cache(self.themes_cache, self.themes_cache_path, self.themes_lock)

        return themes_result

    def generate_wordcloud(self, word_data, title, output_file=None):
        filtered_data = {k: v for k, v in word_data.items() if v > 1}

        if not filtered_data:
            print("没有足够的词生成词云图")
            return False

        wc = WordCloud(
            font_path="simhei.ttf",
            background_color="white",
            width=1200,
            height=800,
            max_words=100
        ).generate_from_frequencies(filtered_data)

        plt.figure(figsize=(12, 8))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.title(title)

        if output_file:
            output_dir = os.path.join(self.cache_dir, "wordclouds")
            os.makedirs(output_dir, exist_ok=True)
            full_path = os.path.join(output_dir, output_file)
            plt.savefig(full_path, dpi=300, bbox_inches="tight")
            print(f"词云图已保存至: {full_path}")
        else:
            plt.show()

        plt.close()
        return True

    def analyze_authors(self, authors_list, force_refresh=False):
        papers = self.load_authors_data(authors_list, force_refresh)

        if not papers:
            print("没有获取到可分析的论文数据")
            return

        keywords = self.extract_keywords(papers, force_refresh)
        self.generate_wordcloud(
            keywords,
            f"作者群关键词词云 ({2015}-{2025})",
            "keywords_cloud.png"
        )

        merged_keywords = self.merge_similar_keywords(keywords, force_refresh)
        self.generate_wordcloud(
            merged_keywords,
            f"合并相似关键词后的词云 ({2015}-{2025})",
            "merged_keywords_cloud.png"
        )

        themes = self.extract_themes_from_abstracts(papers, force_refresh)
        self.generate_wordcloud(
            themes,
            f"从摘要提取的主题词云 ({2015}-{2025})",
            "themes_cloud.png"
        )

        print("\n分析完成！")
        return {
            "papers_count": len(papers),
            "keywords": keywords,
            "merged_keywords": merged_keywords,
            "themes": themes
        }


if __name__ == "__main__":
    AMINER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTg5MjI3OTcsInRpbWVzdGFtcCI6MTc1ODgzNjM5NywidXNlcl9pZCI6IjY4ZDU4OGQxMDc3OTI5ZmI0NjdlOWNmMSJ9.ga0Ftlxf1pSH3-LHjC9MMAT1ATiHpcgH3mnNOGP5R94"
    GLM_API_KEY = "38ef8158834549efa2404f4cb748cf73.fO94Wjp0BxJ80a1T"
    AUTHOR_LIST = [
        {"name": "Boxin Shi", "org": "Peking University"},
        {"name": "Yi Yang", "org": "Zhejiang University"},
        {"name": "Nuno Vasconcelos"},
        {"name": "Abhinav Gupta", "org": "Carnegie Mellon University"},
    ]

    analyzer = AMinerAnalyzer(
        token=AMINER_TOKEN,
        glm_api_key=GLM_API_KEY,
        cache_dir="aminer_cache"
    )

    result = analyzer.analyze_authors(
        authors_list=AUTHOR_LIST,
        force_refresh=False
    )

import requests
import json
import time
import os
from pathlib import Path
import threading
from tqdm import tqdm

class AMinerController:
    def __init__(self, token, cache_dir="aminer_cache"):
        self.token = token
        self.cache_dir = cache_dir

        self.author_id_map_path = os.path.join(cache_dir, "author_id_map.json")
        self.author_papers_map_path = os.path.join(cache_dir, "author_papers_map.json")
        self.paper_details_map_path = os.path.join(cache_dir, "paper_details_map.json")
        self._ensure_cache_files()

        self.id_lock = threading.Lock()
        self.papers_lock = threading.Lock()
        self.details_lock = threading.Lock()

    def _ensure_cache_files(self):
        """确保所有缓存文件存在"""
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(self.author_id_map_path):
            with open(self.author_id_map_path, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
        if not os.path.exists(self.author_papers_map_path):
            with open(self.author_papers_map_path, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
        if not os.path.exists(self.paper_details_map_path):
            with open(self.paper_details_map_path, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)

    def _get_author_key(self, author_name, org=None):
        return f"{author_name}@{org}" if org else author_name

    def _load_author_id_map(self):
        try:
            with open(self.author_id_map_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载作者ID映射失败：{str(e)}，使用空映射")
            return {}

    def _save_author_id_map(self, data):
        with self.id_lock:
            try:
                temp_path = f"{self.author_id_map_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, self.author_id_map_path)
                return True
            except Exception as e:
                print(f"保存作者ID映射失败：{str(e)}")
                return False

    # 根据作者名获取id
    def get_author_id(self, author_name, org=None, force_refresh=False):
        author_key = self._get_author_key(author_name, org)

        if not force_refresh:
            author_map = self._load_author_id_map()
            if author_key in author_map:
                author_id = author_map[author_key]
                print(f"从作者ID映射获取 [{author_name}] 的ID：{author_id}")
                return author_id

        api_url = "https://datacenter.aminer.cn/gateway/open_platform/api/person/search"
        headers = {
            "Content-Type": "application/json;charset=utf-8",
            "Authorization": self.token
        }

        payload = {"name": author_name, "offset": 0, "size": 10}
        if org:
            payload["org"] = org

        for retry in range(3):
            try:
                response = requests.post(
                    api_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=10
                )
                response.raise_for_status()
                result = response.json()

                if result.get("success") and result.get("data"):
                    author_id = result["data"][0]["id"]
                    author_map = self._load_author_id_map()
                    author_map[author_key] = author_id
                    self._save_author_id_map(author_map)
                    print(f"已更新作者ID映射，[{author_name}] 的ID：{author_id}")
                    return author_id
                print(f"未找到作者 [{author_name}] 的匹配结果")
                return None

            except Exception as e:
                print(f"获取作者ID失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(1.5)

        print("超过最大重试次数，获取作者ID失败")
        return None

    def _load_author_papers_map(self):
        try:
            with open(self.author_papers_map_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载作者论文映射失败：{str(e)}，使用空映射")
            return {}

    def _save_author_papers_map(self, data):
        with self.papers_lock:
            try:
                temp_path = f"{self.author_papers_map_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, self.author_papers_map_path)
                return True
            except Exception as e:
                print(f"保存作者论文映射失败：{str(e)}")
                return False

    # 根据作者id获取论文id
    def get_author_papers(self, author_name, org=None, force_refresh=False):
        author_key = self._get_author_key(author_name, org)

        if not force_refresh:
            papers_map = self._load_author_papers_map()
            if author_key in papers_map:
                cached_data = papers_map[author_key]
                actual_count = len(cached_data.get('papers', []))
                if cached_data.get('total_papers') != actual_count:
                    print(f"检测到论文总数({cached_data.get('total_papers')})与实际数量({actual_count})不匹配，将重新获取数据...")
                    force_refresh = True
                else:
                    print(f"从论文映射获取 [{author_name}] 的论文（共{cached_data['total_papers']}篇）")
                    return cached_data

        print(f"开始获取作者 [{author_name}] 的论文信息...")
        author_id = self.get_author_id(author_name, org, force_refresh)
        if not author_id:
            print("作者ID获取失败，终止流程")
            return None

        api_url = "https://datacenter.aminer.cn/gateway/open_platform/api/person/paper/relation"
        headers = {"Authorization": self.token}
        params = {"id": author_id}

        for retry in range(3):
            try:
                response = requests.get(api_url, headers=headers, params=params, timeout=15)
                response.raise_for_status()
                result = response.json()

                if result.get("success") and result.get("data"):
                    papers_data = {
                        "author_name": author_name,
                        "org": org,
                        "author_id": author_id,
                        "total_papers": len(result["data"]),
                        "papers": [{"paper_id": item["id"], "title": item["title"]} for item in result["data"]],
                        "fetch_time": time.strftime("%Y-%m-%d %H:%M:%S")
                    }

                    papers_map = self._load_author_papers_map()
                    papers_map[author_key] = papers_data
                    self._save_author_papers_map(papers_map)
                    print(f"已更新论文映射，[{author_name}] 共 {papers_data['total_papers']} 篇论文")
                    return papers_data
                print("未获取到论文数据")
                return None

            except Exception as e:
                print(f"获取论文失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(2)

        print("超过最大重试次数，获取失败")
        return None

    def save_author_papers(self, author_name, org=None, force_refresh=False):
        result = self.get_author_papers(author_name, org, force_refresh)
        return result is not None

    def _load_paper_details_map(self):
        try:
            with open(self.paper_details_map_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载论文详情映射失败：{str(e)}，使用空映射")
            return {}

    def _save_paper_details_map(self, data):
        with self.details_lock:
            try:
                temp_path = f"{self.paper_details_map_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, self.paper_details_map_path)
                return True
            except Exception as e:
                print(f"保存论文详情映射失败：{str(e)}")
                return False

    # 根据论文id获取论文详情
    def get_paper_details(self, paper_id, force_refresh=False):
        if not force_refresh:
            details_map = self._load_paper_details_map()
            if paper_id in details_map:
                print(f"从详情映射获取论文 [{paper_id}] 的信息")
                return details_map[paper_id]

        api_url = "https://datacenter.aminer.cn/gateway/open_platform/api/paper/detail"
        headers = {"Authorization": self.token}
        params = {"id": paper_id}

        for retry in range(3):
            try:
                response = requests.get(
                    api_url,
                    headers=headers,
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                result = response.json()

                if result.get("success") and result.get("data"):
                    paper_details = result["data"][0]
                    details_map = self._load_paper_details_map()
                    details_map[paper_id] = paper_details
                    self._save_paper_details_map(details_map)
                    return paper_details
                print(f"未找到论文ID [{paper_id}] 的详情")
                return None

            except Exception as e:
                print(f"获取论文详情失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(1.5)

        print(f"论文 [{paper_id}] 超过最大重试次数，获取失败")
        return None

    def save_paper_details(self, paper_id, force_refresh=False):
        result = self.get_paper_details(paper_id, force_refresh)
        return result is not None

    # 根据作者名保存所有的论文详情
    def batch_save_papers(self, author_name, org=None, force_refresh=False):
        print(f"开始批量处理作者 [{author_name}] 的论文详情...")

        author_papers = self.get_author_papers(author_name, org, force_refresh)
        if not author_papers or "papers" not in author_papers:
            print("无法获取作者的论文列表，终止批量操作")
            return None

        actual_count = len(author_papers["papers"])
        if author_papers.get("total_papers") != actual_count:
            print(f"检测到论文总数({author_papers.get('total_papers')})与实际数量({actual_count})不匹配，将重新获取数据后再批量处理...")
            author_papers = self.get_author_papers(author_name, org, force_refresh=True)
            if not author_papers or "papers" not in author_papers:
                print("重新获取后仍无法获取有效论文列表，终止批量操作")
                return None

        total = len(author_papers["papers"])
        success = 0
        fail = 0

        for i, paper in tqdm(enumerate(author_papers["papers"], 1)):
            paper_id = paper["paper_id"]

            if self.save_paper_details(paper_id, force_refresh):
                success += 1
            else:
                fail += 1

            time.sleep(0.5 if i % 10 != 0 else 2)

        print(f"\n批量处理完成：总{total}篇，成功{success}篇，失败{fail}篇")
        return {
            "total": total,
            "success": success,
            "fail": fail,
            "cache_file": self.paper_details_map_path
        }

    # 执行命令
    def execute(self, command, **kwargs):
        commands = {
            "get_author_papers": self.get_author_papers,
            "get_paper_details": self.get_paper_details,
            "batch_save_papers": self.batch_save_papers,
            "get_author_id": self.get_author_id
        }

        if command not in commands:
            raise ValueError(f"不支持的命令: {command}，支持的命令有: {list(commands.keys())}")

        return commands[command](** kwargs)


# ------------------------------
# 使用示例
# ------------------------------
if __name__ == "__main__":
    AMINER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTk2OTM1MjYsInRpbWVzdGFtcCI6MTc1ODgyOTUyNiwidXNlcl9pZCI6IjY4ZDU4OGQxMDc3OTI5ZmI0NjdlOWNmMSJ9.SMQjNjejJgtjG2loDsH4669BqH3tsv2xg3SrQEoWhTA"
    controller = AMinerController(
        token=AMINER_TOKEN,
        cache_dir="aminer_cache"
    )

    # 1. 获取并保存作者论文信息
    # controller.execute(
    #     command="get_author_papers",
    #     author_name="R Wagner",
    #     # force_refresh=True
    # )

    # 2. 批量保存论文详情（自动更新到映射文件）
    controller.execute(
        command="batch_save_papers",
        author_name="A Min Tjoa"
    )

    # 3. 单独获取论文详情
    # paper_details = controller.execute(
    #     command="get_paper_details",
    #     paper_id="替换为实际论文ID"
    # )
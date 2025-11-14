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

        self.author_id_map = self._load_cache(self.author_id_map_path)
        self.author_papers_map = self._load_cache(self.author_papers_map_path)
        self.paper_details_map = self._load_cache(self.paper_details_map_path)
        self.id_lock = threading.Lock()
        self.papers_lock = threading.Lock()
        self.details_lock = threading.Lock()

    def _ensure_cache_files(self):
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        for path in [self.author_id_map_path, self.author_papers_map_path, self.paper_details_map_path]:
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)

    def _load_cache(self, file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载缓存文件 {file_path} 失败：{str(e)}，使用空字典")
            return {}

    def _save_cache(self, data, file_path, lock):
        with lock:
            try:
                temp_path = f"{file_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                os.replace(temp_path, file_path)
                return True
            except Exception as e:
                print(f"保存缓存文件 {file_path} 失败：{str(e)}")
                return False

    def _get_author_key(self, author_name, org=None):
        """生成作者唯一标识键"""
        return f"{author_name}@{org}" if org else author_name

    # 根据作者名获取id
    def get_author_id(self, author_name, org=None, force_refresh=False):
        author_key = self._get_author_key(author_name, org)

        if not force_refresh and author_key in self.author_id_map:
            author_id = self.author_id_map[author_key]
            print(f"从内存缓存获取 [{author_name}] 的ID：{author_id}")
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
                    print(result["data"][0]["org"])
                    print(result["data"][0]["id"])
                    print(result["data"][0]["name"])
                    # if result["data"][0]["name"] == author_name:
                    author_id = result["data"][0]["id"]
                    self.author_id_map[author_key] = author_id
                    self._save_cache(self.author_id_map, self.author_id_map_path, self.id_lock)
                    print(f"已更新 [{author_name}] 的ID：{author_id}")
                    return author_id
                print(f"未找到作者 [{author_name}] 的匹配结果")
                return None

            except Exception as e:
                print(f"获取作者ID失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(1.5)

        print("超过最大重试次数，获取作者ID失败")
        return None

    # 根据作者id获取论文id
    def get_author_papers(self, author_name, org=None, force_refresh=False):
        author_key = self._get_author_key(author_name, org)

        if not force_refresh and author_key in self.author_papers_map:
            cached_data = self.author_papers_map[author_key]
            actual_count = len(cached_data.get('papers_old', []))
            if cached_data.get('total_papers') != actual_count:
                print(f"检测到论文数量不匹配，将重新获取数据...")
                force_refresh = True
            else:
                print(f"从内存缓存获取 [{author_name}] 的论文（共{cached_data['total_papers']}篇）")
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
                        "papers_old": [{"paper_id": item["id"], "title": item["title"]} for item in result["data"]],
                        "fetch_time": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.author_papers_map[author_key] = papers_data
                    self._save_cache(self.author_papers_map, self.author_papers_map_path, self.papers_lock)
                    print(f"已更新 [{author_name}] 的论文数据（共{papers_data['total_papers']}篇）")
                    return papers_data
                print("未获取到论文数据")
                return None

            except Exception as e:
                print(f"获取论文失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(2)

        print("超过最大重试次数，获取失败")
        return None

    def get_paper_details(self, paper_id, force_refresh=False):
        if not force_refresh and paper_id in self.paper_details_map:
            return self.paper_details_map[paper_id]

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
                # print(result)
                if result.get("success") and result.get("data"):
                    paper_details = result["data"][0]
                    self.paper_details_map[paper_id] = paper_details
                    self._save_cache(self.paper_details_map, self.paper_details_map_path, self.details_lock)
                    return paper_details
                print(f"未找到论文ID [{paper_id}] 的详情")
                return None

            except Exception as e:
                print(f"获取论文详情失败（第{retry + 1}次重试）：{str(e)}")
                time.sleep(1.5)

        print(f"论文 [{paper_id}] 超过最大重试次数，获取失败")
        return None

    def batch_save_papers(self, author_name, org=None, force_refresh=False):
        print(f"开始批量处理作者 [{author_name}] 的论文详情...")

        author_papers = self.get_author_papers(author_name, org, force_refresh)
        if not author_papers or "papers_old" not in author_papers:
            print("无法获取作者的论文列表，终止批量操作")
            return None

        actual_count = len(author_papers["papers_old"])
        if author_papers.get("total_papers") != actual_count:
            print(f"检测到论文数量不匹配，重新获取数据后处理...")
            author_papers = self.get_author_papers(author_name, org, force_refresh=True)
            if not author_papers or "papers_old" not in author_papers:
                print("重新获取后仍无法获取有效论文列表，终止操作")
                return None

        total = len(author_papers["papers_old"])
        success = 0
        fail = 0

        papers_to_fetch = []
        for paper in author_papers["papers_old"]:
            paper_id = paper["paper_id"]
            if force_refresh or paper_id not in self.paper_details_map:
                papers_to_fetch.append(paper_id)
            else:
                success += 1  # 已在缓存中
        print(f"需处理 {len(papers_to_fetch)} 篇新论文，{success} 篇已在缓存中")

        for paper_id in tqdm(papers_to_fetch, desc="获取论文详情"):
            try:
                result = self.get_paper_details(paper_id, force_refresh)
                if result is not None:
                    success += 1
                else:
                    fail += 1
            except Exception as e:
                print(f"处理论文 {paper_id} 时发生异常: {str(e)}")
                fail += 1

        # for i, papers in tqdm(enumerate(author_papers["papers_old"], 1)):
        #     paper_id = papers["paper_id"]
        #     if self.get_paper_details(paper_id, force_refresh) is not None:
        #         success += 1
        #     else:
        #         fail += 1
        #
        #     time.sleep(0.5 if i % 10 != 0 else 2)

        print(f"\n批量处理完成：总{total}篇，成功{success}篇，失败{fail}篇")
        return {
            "total": total,
            "success": success,
            "fail": fail,
            "cache_file": self.paper_details_map_path
        }

    def execute(self, command, **kwargs):
        commands = {
            "get_author_papers": self.get_author_papers,
            "get_paper_details": self.get_paper_details,
            "batch_save_papers": self.batch_save_papers,
            "get_author_id": self.get_author_id
        }

        if command not in commands:
            raise ValueError(f"不支持的命令: {command}，支持的命令有: {list(commands.keys())}")

        return commands[command](**kwargs)

if __name__ == "__main__":
    AMINER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTg5MjI3OTcsInRpbWVzdGFtcCI6MTc1ODgzNjM5NywidXNlcl9pZCI6IjY4ZDU4OGQxMDc3OTI5ZmI0NjdlOWNmMSJ9.ga0Ftlxf1pSH3-LHjC9MMAT1ATiHpcgH3mnNOGP5R94"
    controller = AMinerController(
        token=AMINER_TOKEN,
        cache_dir="aminer_cache"
    )

    controller.execute(
        command="get_author_id",
        author_name="Nuno Vasconcelos",
        force_refresh=True
    )


    # controller.execute(
    #     command="get_paper_details",
    #     paper_id="654dc9a2939a5f4082c1d13a",
    #     force_refresh=False
    # )

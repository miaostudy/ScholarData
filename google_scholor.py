import requests
import os
import re

# 代理设置（根据实际情况调整或移除）
os.environ['http_proxy'] = '127.0.0.1:7890'
os.environ['https_proxy'] = '127.0.0.1:7890'


def extract_year(publication_info):
    """从发表信息中提取年份"""
    if not publication_info:
        return ""
    # 正则匹配年份（4位数字）
    year_match = re.search(r'\b(20\d{2}|19\d{2})\b', publication_info)
    return year_match.group(0) if year_match else ""


def get_scholar_papers(scholar_id, api_key, max_pages=3):
    base_url = "https://serpapi.com/search.json"
    params = {
        "engine": "google_scholar",
        "q": f"author:{scholar_id}",
        "api_key": api_key,
        "num": 20,
        "as_sdt": 0
    }
    all_papers = []

    for page in range(max_pages):
        params["start"] = page * 20
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("error"):
                print(f"API错误: {data['error']}")
                break


            for paper in data.get("organic_results", []):
                publication_info = paper.get("publication_info", {}).get("summary", "")
                paper_info = {
                    "title": paper.get("title", "无标题"),
                    "abstract": paper.get("snippet", "无摘要信息"),
                    "link": paper.get("link", "无链接"),
                    "publication": publication_info,
                    "citations": paper.get("inline_links", {})
                    .get("cited_by", {})
                    .get("total", 0),
                    "year": extract_year(publication_info),
                    "versions": paper.get("inline_links", {})
                    .get("versions", {})
                    .get("total", 0)
                }
                all_papers.append(paper_info)

            if not data.get("pagination", {}).get("next"):
                print(f"已获取所有结果（共{len(all_papers)}篇）")
                break

        except requests.exceptions.RequestException as e:
            print(f"请求失败: {str(e)}")
            break

    return all_papers


# 示例调用
if __name__ == "__main__":
    scholar_id = "R Wagner"
    api_key = "9297896123b17bbb9334f5f203b8eb7a09da3e0b37883eac0708d90e4badee5b"

    papers = get_scholar_papers(scholar_id, api_key, max_pages=100)

    print(f"\n共找到 {len(papers)} 篇论文：")
    for i, paper in enumerate(papers[:5], 1):
        print(f"\n论文 {i}:")
        print(f"标题: {paper['title']}")
        print(f"摘要: {paper['abstract']}")
        print(f"发表信息: {paper['publication']}")
        print(f"引用量: {paper['citations']}")
        print(f"链接: {paper['link']}")

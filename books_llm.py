import os
import json
import shutil
import pytesseract
import requests
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import time

# 配置参数
TESSERACT_PATH = r'D:\tesseract\tesseract.exe'
GLM_API_KEY = "38ef8158834549efa2404f4cb748cf73.fO94Wjp0BxJ80a1T"
# 超时与重试设置
API_TIMEOUT = 300
API_RETRY = 3  # 增加重试次数，提高成功率


class PDFTOCExtractorWithLLM:
    def __init__(self, books_dir="./books", cache_dir="./books_cache", failed_dir="./filed"):
        self.books_dir = books_dir
        self.cache_dir = cache_dir
        self.failed_dir = failed_dir

        self.book_cache_file = os.path.join(cache_dir, "toc_cache.json")
        self.page_cache_file = os.path.join(cache_dir, "page_cache.json")
        self.failed_record_file = os.path.join(cache_dir, "failed_books.json")  # 失败记录是列表

        self.glm_api_key = GLM_API_KEY

        # 创建目录
        os.makedirs(books_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs(failed_dir, exist_ok=True)

        # 加载缓存：book_cache是字典，page_cache是字典，failed_records是列表
        self.book_cache = self._load_json(self.book_cache_file, default={})
        self.page_cache = self._load_json(self.page_cache_file, default={})
        self.failed_records = self._load_json(self.failed_record_file, default=[])  # 修复：失败记录为列表

        pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

    def _load_json(self, file_path, default):
        """通用JSON加载方法，支持指定默认类型（字典/列表）"""
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"加载{file_path}出错: {e}，使用默认{type(default).__name__}")
        return default  # 失败时返回默认类型（字典/列表）

    def _save_json(self, data, file_path):
        """通用JSON保存方法"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"已保存到 {file_path}")
        except IOError as e:
            print(f"保存{file_path}出错: {e}")

    def _is_book_processed(self, filename):
        """判断书籍是否已处理"""
        return filename in self.book_cache

    def _get_page_cache_key(self, filename, page_num):
        """生成单页缓存的唯一键"""
        return f"{filename}_{page_num}"

    def _get_page_cache(self, filename, page_num):
        """获取单页缓存"""
        key = self._get_page_cache_key(filename, page_num)
        return self.page_cache.get(key, None)

    def _set_page_cache(self, filename, page_num, data):
        """设置单页缓存（增加success标识）"""
        key = self._get_page_cache_key(filename, page_num)
        data["success"] = data.get("success", False)
        self.page_cache[key] = data
        self._save_json(self.page_cache, self.page_cache_file)

    def _copy_failed_file(self, src_path, filename):
        """复制失败文件到指定目录"""
        try:
            dst_path = os.path.join(self.failed_dir, filename)
            if not os.path.exists(dst_path):
                shutil.copy2(src_path, dst_path)
                print(f"失败文件已复制到: {dst_path}")
            return True
        except Exception as e:
            print(f"复制失败文件 {filename} 出错: {e}")
            return False

    def _pdf_page_to_image(self, pdf_path, page_num, dpi=200):
        """PDF页转图片"""
        try:
            images = convert_from_path(
                pdf_path,
                dpi=dpi,
                first_page=page_num + 1,
                last_page=page_num + 1,
                fmt='png',
                grayscale=True,
                thread_count=4
            )
            return images[0] if images else None
        except Exception as e:
            print(f"第{page_num + 1}页转图片失败: {e}")
            return None

    def _ocr_image(self, image):
        """OCR识别图片文本"""
        try:
            threshold = 180
            image = image.point(lambda p: p > threshold and 255)
            max_size = (1500, 1500)
            image.thumbnail(max_size)
            return pytesseract.image_to_string(image, lang='chi_sim+eng').strip()
        except Exception as e:
            print(f"OCR识别失败: {e}")
            return ""

    def _extract_single_page_text(self, pdf_path, page_num):
        """提取单页文本"""
        try:
            image = self._pdf_page_to_image(pdf_path, page_num)
            if not image:
                return ""
            return self._ocr_image(image)
        except Exception as e:
            print(f"第{page_num + 1}页文本提取失败: {e}")
            return ""

    def _call_glm_api(self, prompt, system_message=None):
        """大模型调用（带重试）"""
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
            "response_format": {"type": "json_object"}
        }

        for retry in range(API_RETRY + 1):
            try:
                response = requests.post(
                    api_url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=API_TIMEOUT
                )
                response.raise_for_status()
                result = response.json()

                if "choices" in result and len(result["choices"]) > 0:
                    return result["choices"][0]["message"]["content"]
                else:
                    print(f"大模型返回格式异常（重试{retry}/{API_RETRY}）")
            except Exception as e:
                print(f"大模型调用失败（重试{retry}/{API_RETRY}）：{e}")
                if retry < API_RETRY:
                    time.sleep(3)
        return None

    def _judge_page_is_toc(self, filename, page_text, page_num):
        """判断单页是否为目录（缓存逻辑优化）"""
        page_cache = self._get_page_cache(filename, page_num)

        # 缓存存在且有效时使用
        if page_cache and page_cache.get("success", False):
            print(page_cache.get("success", False))
            return page_cache.get("is_toc", False)
        print("api")
        # 重新调用API
        if not page_text:
            self._set_page_cache(filename, page_num, {
                "is_toc": False,
                "toc_items": [],
                "success": True
            })
            return False

        prompt = f"""
        任务：根据当前页面内容判断该页面是否是目录页（含有章节标题+对应页码）。
        当前页面内容（第{page_num + 1}页）：
        {page_text}
        
        要求：
        - 是目录页返回：{{"is_toc": true}}
        - 不是目录页返回：{{"is_toc": false}}
        - 仅返回JSON，无其他内容
        """
        system_message = "你是目录页识别工具，可以根据当前页面的内容判断该页面是否为目录页。"
        response = self._call_glm_api(prompt, system_message)

        is_toc = False
        success = False
        if response:
            try:
                result = json.loads(response)
                is_toc = result.get("is_toc", False)
                success = True
            except:
                print(f"第{page_num + 1}页判断格式错误")

        self._set_page_cache(filename, page_num, {
            "is_toc": is_toc,
            "toc_items": [],
            "success": success
        })
        return is_toc

    def _extract_toc_from_single_page(self, filename, page_text, page_num):
        """单页提取目录"""
        page_cache = self._get_page_cache(filename, page_num)
        print(page_text)
        # 缓存存在且有效时使用
        if page_cache and page_cache.get("success", False) and len(page_cache.get("toc_items", [])) > 0:
            return page_cache["toc_items"]

        # 重新调用API
        if not page_text:
            self._set_page_cache(filename, page_num, {
                "is_toc": False,
                "toc_items": [],
                "success": True
            })
            return []

        prompt = f"""
        任务：以下文本是一本书的单页OCR结果，请从中提取目录项。
        文本内容（第{page_num + 1}页）：
        {page_text}
        
        要求：
        - 提取有意义的章节标题(title)、层级(level)和对应页码(转换为阿拉伯数字), 若没有页码则将页码设为-1
        - 可以纠正错别字、清理特殊符号
        - 我们的最终目标是分析知识实体，可以删除"本章小结"等无意义内容
        - 格式{{"toc": [{{"level":int,"title":str,"page":int}}]}}
        - 仅返回JSON，无其他内容
        """
        system_message = "你是目录内容提取专家，擅长提取有意义的目录项。"
        response = self._call_glm_api(prompt, system_message)

        toc_items = []
        success = False
        if response:
            try:
                result = json.loads(response)
                toc = result.get("toc", [])
                for item in toc:
                    if all(k in item for k in ["level", "title", "page"]) and \
                            isinstance(item["level"], int) and isinstance(item["page"], int):
                        toc_items.append(item)
                success = True
            except:
                print(f"第{page_num + 1}页提取格式错误")

        current_cache = self._get_page_cache(filename, page_num) or {}
        self._set_page_cache(filename, page_num, {
            "is_toc": current_cache.get("is_toc", True),
            "toc_items": toc_items,
            "success": success
        })
        print(toc_items)
        return toc_items

    def process_all_books(self):
        pdf_files = [f for f in os.listdir(self.books_dir) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"未在 {self.books_dir} 找到PDF文件")
            return

        print(f"发现 {len(pdf_files)} 个PDF文件，开始处理...")

        for filename in pdf_files:
            if self._is_book_processed(filename):
                print(f"{filename} 已处理，跳过")
                continue

            print(f"\n===== 开始处理 {filename} =====")
            pdf_path = os.path.join(self.books_dir, filename)
            try:
                total_pages = len(PdfReader(pdf_path).pages)
            except Exception as e:
                print(f"无法读取PDF: {e}")
                # 失败记录是列表，使用append添加
                self.failed_records.append({
                    "filename": filename,
                    "reason": "PDF文件损坏或无法读取",
                })
                self._copy_failed_file(pdf_path, filename)
                self._save_json(self.failed_records, self.failed_record_file)
                continue

            max_process = min(80, total_pages)
            all_toc_items = []
            in_toc_region = False

            for page_num in range(9, max_process):
                print(f"\n处理第{page_num + 1}/{max_process}页...")

                page_text = self._extract_single_page_text(pdf_path, page_num)
                if not page_text:
                    print(f"第{page_num + 1}页无文本，跳过")
                    if in_toc_region:
                        break
                    continue

                if not in_toc_region:
                    is_toc = self._judge_page_is_toc(filename, page_text, page_num)
                    print(is_toc)
                    if is_toc:
                        print(f"第{page_num + 1}页检测到目录，开始提取...")
                        in_toc_region = True
                        toc_items = self._extract_toc_from_single_page(filename, page_text, page_num)
                        if toc_items:
                            all_toc_items.extend(toc_items)
                            print(f"第{page_num + 1}页提取到 {len(toc_items)} 条目录")
                else:
                    is_toc = self._judge_page_is_toc(filename, page_text, page_num)
                    if is_toc:
                        print(f"第{page_num + 1}页仍为目录页，继续提取...")
                        toc_items = self._extract_toc_from_single_page(filename, page_text, page_num)
                        if toc_items:
                            all_toc_items.extend(toc_items)
                            print(f"第{page_num + 1}页提取到 {len(toc_items)} 条目录")
                    else:
                        print(f"第{page_num + 1}页非目录页，目录区域结束")
                        break

            if all_toc_items:
                # 去重排序
                seen = set()
                unique_toc = []
                for item in all_toc_items:
                    key = (item["title"], item["page"])
                    if key not in seen:
                        seen.add(key)
                        unique_toc.append(item)
                unique_toc.sort(key=lambda x: x["page"])

                self.book_cache[filename] = {
                    "filename": filename,
                    "toc": unique_toc,
                }
                print(f"成功提取 {filename} 的目录，共 {len(unique_toc)} 条（去重后）")
            else:
                reason = "未检测到有效目录"
                # 失败记录是列表，使用append添加
                self.failed_records.append({
                    "filename": filename,
                    "reason": reason,
                })
                self._copy_failed_file(pdf_path, filename)
                print(reason)

            # 每本书处理完保存缓存
            self._save_json(self.book_cache, self.book_cache_file)
            self._save_json(self.failed_records, self.failed_record_file)
            print(f"===== 结束处理 {filename} =====\n")

        print("所有文件处理完毕")


if __name__ == "__main__":
    extractor = PDFTOCExtractorWithLLM()
    extractor.process_all_books()

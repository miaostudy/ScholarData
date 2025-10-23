import os
import json
import re
import shutil  # 用于文件复制
import pytesseract
from datetime import datetime
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
from PIL import Image
from roman import fromRoman

# Tesseract路径
pytesseract.pytesseract.tesseract_cmd = r'D:\tesseract\tesseract.exe'


class PDFTOCExtractorOCR:
    def __init__(self, books_dir="./books", cache_dir="./books_cache", failed_dir="./filed"):
        self.books_dir = books_dir
        self.cache_dir = cache_dir
        self.failed_dir = failed_dir  # 失败文件存放目录
        self.cache_file = os.path.join(cache_dir, "toc_cache_ocr.json")
        self.failed_record_file = os.path.join(cache_dir, "failed_books.json")  # 失败记录JSON

        # 创建必要目录
        os.makedirs(books_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs(failed_dir, exist_ok=True)  # 确保失败目录存在

        self.processed_data = self._load_cache()
        self.failed_records = self._load_failed_records()  # 加载失败记录

    def _load_cache(self):
        """加载已处理的缓存数据"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"加载缓存出错: {e}，将创建新缓存")
        return {}

    def _load_failed_records(self):
        """加载失败文件记录"""
        if os.path.exists(self.failed_record_file):
            try:
                with open(self.failed_record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"加载失败记录出错: {e}，将创建新记录")
        return []  # 用列表存储失败记录

    def _save_cache(self):
        """保存缓存数据"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_data, f, ensure_ascii=False, indent=2)
            print(f"缓存已保存到 {self.cache_file}")
        except IOError as e:
            print(f"保存缓存出错: {e}")

    def _save_failed_records(self):
        """保存失败文件记录到JSON"""
        try:
            with open(self.failed_record_file, 'w', encoding='utf-8') as f:
                json.dump(self.failed_records, f, ensure_ascii=False, indent=2)
            print(f"失败记录已保存到 {self.failed_record_file}")
        except IOError as e:
            print(f"保存失败记录出错: {e}")

    def _is_processed(self, filename):
        """检查文件是否已处理"""
        return filename in self.processed_data

    def _copy_failed_file(self, src_path, filename):
        """复制失败的PDF到filed目录"""
        try:
            dst_path = os.path.join(self.failed_dir, filename)
            # 避免重复复制（如果已存在则跳过）
            if not os.path.exists(dst_path):
                shutil.copy2(src_path, dst_path)  # 保留文件元数据
                print(f"已将失败文件复制到: {dst_path}")
            return True
        except Exception as e:
            print(f"复制失败文件 {filename} 出错: {e}")
            return False

    def _pdf_page_to_image(self, pdf_path, page_num, dpi=300):
        """单页PDF转图片"""
        try:
            images = convert_from_path(
                pdf_path,
                dpi=dpi,
                first_page=page_num + 1,
                last_page=page_num + 1,
                fmt='png',
                grayscale=True
            )
            return images[0] if images else None
        except Exception as e:
            print(f"第{page_num + 1}页转图片失败: {e}")
            return None

    def _ocr_image(self, image):
        """OCR识别单张图片"""
        try:
            threshold = 180
            image = image.point(lambda p: p > threshold and 255)
            text = pytesseract.image_to_string(image, lang='chi_sim+eng')
            return text.strip()
        except Exception as e:
            print(f"OCR识别失败: {e}")
            return ""

    def _extract_text_from_scanned_pdf(self, pdf_path, max_pages=80):
        """逐页识别文本"""
        total_pages = len(PdfReader(pdf_path).pages)
        max_process = min(max_pages, total_pages)
        page_texts = []
        toc_started = False
        toc_end = False

        for page_num in range(max_process):
            if toc_end:
                break

            print(f"\n处理第{page_num + 1}/{max_process}页...")
            image = self._pdf_page_to_image(pdf_path, page_num)
            if not image:
                page_texts.append("")
                continue

            # 可视化当前页
            # image.show(title=f"第{page_num + 1}页")

            text = self._ocr_image(image)

            print(text)

            page_texts.append(text)

            if not toc_started:
                if any(kw in text.lower() for kw in ["目录", "contents", "目次"]):
                    toc_started = True
                    print("已定位到目录区域")
            else:
                end_keywords = {"前言", "序言", "第一章", "第1章", "正文", "引言"}
                if any(kw in text for kw in end_keywords):
                    toc_end = True
                    print("检测到目录结束标志，停止处理")

        return page_texts

    def _detect_toc_from_ocr_text(self, page_texts):
        """提取目录"""
        toc = []
        toc_keywords = {"目录", "contents", "目次", "table of contents"}
        toc_start_idx = 0
        toc_end_idx = min(10, len(page_texts) - 1)
        for i, text in enumerate(page_texts):
            if any(keyword in text.lower() for keyword in toc_keywords):
                toc_start_idx = i
                toc_end_idx = min(i + 10, len(page_texts) - 1)
                break

        for page_idx in range(toc_start_idx, toc_end_idx + 1):
            text = page_texts[page_idx]
            if not text:
                continue
            lines = [line.strip() for line in re.split(r'[\n\r]+', text) if line.strip()]
            for line in lines:
                if len(line) < 6:
                    continue

                pattern = r'^(.*?)([\s\.\*—\-/\\]+)([ivxlcdmIVXLCDM\d零一二三四五六七八九十百千]+)$'
                match = re.match(pattern, line)
                if not match:
                    continue

                title_part, _, page_part = match.groups()
                title = title_part.strip()
                if not title:
                    continue

                page_num = self._parse_page_number(page_part)
                if not page_num:
                    continue

                level = self._parse_level(title)
                toc.append({"level": level, "title": title, "page": page_num})

        unique_toc = []
        seen = set()
        for item in toc:
            key = (item["title"], item["page"])
            if key not in seen:
                seen.add(key)
                unique_toc.append(item)
        return sorted(unique_toc, key=lambda x: x["page"])

    def _parse_page_number(self, page_str):
        """解析页码"""
        cn_num_map = {
            '零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
            '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
            '百': 100, '千': 1000
        }

        try:
            if page_str.isdigit():
                return int(page_str)
            return fromRoman(page_str.upper())
        except:
            try:
                num = 0
                current = 0
                for c in page_str:
                    if c in cn_num_map:
                        val = cn_num_map[c]
                        if val >= 10:
                            if current == 0:
                                current = 1
                            num += current * val
                            current = 0
                        else:
                            current += val
                num += current
                return num if num > 0 else None
            except:
                return None

    def _parse_level(self, title):
        """识别目录层级"""
        if re.match(r'^(\d+\.){2,}', title):
            return len(re.findall(r'\d+\.', title))
        elif re.match(r'^(\s{4,}|\t{2,})', title):
            return 3
        elif re.match(r'^(\s{2,}|\t)', title):
            return 2
        elif re.match(r'^[一二三四五六七八九十]+、', title):
            return 1
        elif re.match(r'^[①②③④⑤⑥⑦⑧⑨⑩]', title):
            return 2
        else:
            return 1

    def process_all_books(self):
        """批量处理PDF并记录失败文件"""
        pdf_files = [f for f in os.listdir(self.books_dir) if f.lower().endswith('.pdf')]
        failed_files = []  # 记录本次处理失败的文件

        if not pdf_files:
            print(f"未在 {self.books_dir} 找到PDF文件")
            return

        print(f"发现 {len(pdf_files)} 个PDF文件，开始OCR处理...")

        for filename in pdf_files:
            if self._is_processed(filename):
                print(f"{filename} 已处理，跳过")
                continue

            print(f"\n===== 开始处理 {filename} =====")
            pdf_path = os.path.join(self.books_dir, filename)
            failure_reason = ""  # 记录失败原因

            # 提取文本
            page_texts = self._extract_text_from_scanned_pdf(pdf_path, max_pages=50)
            if not page_texts:
                failure_reason = "无法转换PDF为图片"
                print(f"{filename} {failure_reason}，标记为失败")
                failed_files.append((filename, failure_reason))
                continue

            # 提取目录
            toc = self._detect_toc_from_ocr_text(page_texts)
            if toc:
                self.processed_data[filename] = {
                    "filename": filename,
                    "path": pdf_path,
                    "toc": toc,
                    "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "is_scanned": True
                }
                print(f"成功提取 {filename} 的目录，共 {len(toc)} 条")
            else:
                failure_reason = "OCR识别后未提取到目录"
                print(f"{filename} {failure_reason}，标记为失败")
                failed_files.append((filename, failure_reason))

            print(f"===== 结束处理 {filename} =====\n")

        # 处理失败文件：复制到filed目录并记录
        for filename, reason in failed_files:
            src_path = os.path.join(self.books_dir, filename)
            # 复制文件
            self._copy_failed_file(src_path, filename)
            # 记录到失败列表（包含时间和原因）
            self.failed_records.append({
                "filename": filename,
                "failure_reason": reason,
                "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        # 保存缓存和失败记录
        self._save_cache()
        self._save_failed_records()
        print("所有文件处理完毕")


if __name__ == "__main__":
    extractor = PDFTOCExtractorOCR()
    extractor.process_all_books()

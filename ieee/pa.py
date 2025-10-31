from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, \
    StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
import random
import os
from urllib.parse import urljoin, urlparse, parse_qs
import logging
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

# 创建保存JSON缓存的目录
if not os.path.exists('json_cache'):
    os.makedirs('json_cache')

# 创建保存不同类型数据的子目录
for dir_name in ['decades', 'years', 'issues', 'papers']:
    if not os.path.exists(f'json_cache/{dir_name}'):
        os.makedirs(f'json_cache/{dir_name}')


class IEEEScraper:
    def __init__(self, punumber, use_cache=True):
        self.punumber = punumber
        self.base_url = "https://ieeexplore.ieee.org"
        self.issues_url = f"{self.base_url}/xpl/issues?punumber={punumber}"
        self.all_papers = []
        self.all_isnumbers = set()  # 使用集合避免重复
        self.use_cache = use_cache  # 缓存开关

        # 初始化Selenium浏览器
        options = webdriver.ChromeOptions()
        # 取消注释以启用无头模式
        # options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument(
            f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36')

        # 添加更多模拟真实浏览器的设置
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.driver.implicitly_wait(10)  # 隐式等待时间

        # 移除webdriver特征
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def _save_json(self, data, filename):
        """保存数据到JSON文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logging.info(f"数据已保存到 {filename}")
        except Exception as e:
            logging.error(f"保存JSON失败: {str(e)}")

    def _load_cached_json(self, filename):
        """从JSON缓存文件加载数据"""
        try:
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logging.info(f"从缓存加载数据: {filename}")
                return data
            return None
        except Exception as e:
            logging.error(f"加载缓存JSON失败: {str(e)}")
            return None

    def _get_soup(self, url, wait_for=None):
        """使用Selenium获取页面并返回BeautifulSoup对象，不处理缓存"""
        try:
            # 添加随机延迟，避免被反爬
            time.sleep(random.uniform(2, 4))
            self.driver.get(url)
            logging.info(f"获取页面 {url}")

            # 等待指定元素加载完成
            if wait_for:
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located(wait_for)
                )
            else:
                # 默认等待body加载完成
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

            # 对于动态加载的内容，可能需要额外等待
            time.sleep(random.uniform(2, 5))

            # 获取页面内容
            page_source = self.driver.page_source

            # 检查响应内容
            if not page_source.strip():
                logging.warning(f"页面 {url} 内容为空")
                return None

            return BeautifulSoup(page_source, 'html.parser')
        except TimeoutException:
            logging.error(f"获取页面 {url} 超时")
            return None
        except Exception as e:
            logging.error(f"获取页面 {url} 失败: {str(e)}")
            return None

    def _click_and_wait(self, element_locator, wait_condition=None, retry=3):
        """
        点击元素并等待指定条件满足
        element_locator: 元素定位器
        wait_condition: 等待条件，如EC.presence_of_element_located
        retry: 重试次数
        """
        try:
            # 等待元素可点击
            element = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable(element_locator)
            )

            # 滚动到元素可见位置
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(random.uniform(1, 2))  # 等待滚动完成

            # 点击元素
            element.click()
            logging.info(f"点击元素: {element_locator}")

            # 如果有指定等待条件，则等待其满足
            if wait_condition:
                WebDriverWait(self.driver, 20).until(wait_condition)
                time.sleep(random.uniform(1, 3))  # 等待内容加载

            return True
        except (TimeoutException, ElementClickInterceptedException, StaleElementReferenceException) as e:
            logging.warning(f"点击元素 {element_locator} 失败: {str(e)}")
            if retry > 0:
                # 等待一会儿再重试
                time.sleep(random.uniform(2, 4))
                # 尝试使用JavaScript点击
                try:
                    # 重新查找元素，避免元素过时
                    element = self.driver.find_element(*element_locator)
                    self.driver.execute_script("arguments[0].click();", element)
                    logging.info(f"使用JavaScript点击元素: {element_locator}")

                    if wait_condition:
                        WebDriverWait(self.driver, 20).until(wait_condition)
                        time.sleep(random.uniform(1, 3))
                    return True
                except Exception as js_e:
                    logging.warning(f"JavaScript点击元素 {element_locator} 失败: {str(js_e)}")
                    return self._click_and_wait(element_locator, wait_condition, retry - 1)
            return False

    def get_all_isnumbers(self, force_refresh=False):
        """获取所有年代、所有年份的期号isnumber，严格按照先年代后年份的顺序"""
        logging.info("开始获取所有年代、所有年份的isnumber...")

        # 检查整体isnumber缓存
        isnumbers_cache_file = 'json_cache/all_isnumbers.json'
        if self.use_cache and os.path.exists(isnumbers_cache_file) and not force_refresh:
            cached_data = self._load_cached_json(isnumbers_cache_file)
            if cached_data:
                return cached_data

        # 首先加载期刊列表页面，等待年代标签加载完成
        self._get_soup(
            self.issues_url,
            wait_for=(By.CSS_SELECTOR, 'div.issue-details-past-tabs:not(.year)')  # 等待年代标签容器
        )

        # 定位年代标签（2020s, 2010s等）
        try:
            decade_elements = WebDriverWait(self.driver, 15).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, 'div.issue-details-past-tabs:not(.year) li a')
                )
            )
            decade_names = [elem.text.strip() for elem in decade_elements if elem.text.strip()]
            logging.info(f"找到年代标签: {decade_names}")
        except Exception as e:
            logging.error(f"获取年代标签失败: {str(e)}")
            return []

        # 用于验证点击后页面是否变化的元素 - 期号容器
        issue_container_selector = (By.CSS_SELECTOR, 'xpl-past-issue-list section.issue-container')

        # 遍历每个年代
        for decade_name in tqdm(decade_names, desc="处理年代"):
            logging.info(f"\n===== 开始处理年代: {decade_name} =====")

            # 检查年代缓存是否存在
            decade_cache_file = f'json_cache/decades/decade_{decade_name}.json'
            if self.use_cache and os.path.exists(decade_cache_file) and not force_refresh:
                # 从缓存加载年代数据
                decade_data = self._load_cached_json(decade_cache_file)
                if decade_data and 'years' in decade_data:
                    year_names = decade_data['years']
                    logging.info(f"从缓存加载年代 {decade_name} 的年份: {year_names}")
                else:
                    year_names = []
            else:
                # 点击年代标签，等待年份标签出现
                decade_locator = (By.XPATH,
                                  f'//div[contains(@class, "issue-details-past-tabs") and not(contains(@class, "year"))]//a[text()="{decade_name}"]'
                                  )

                # 等待条件：年份标签容器出现
                year_container_condition = EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'div.issue-details-past-tabs.year')
                )

                if not self._click_and_wait(decade_locator, year_container_condition):
                    logging.error(f"无法点击年代标签 {decade_name}，跳过该年代")
                    continue

                # 获取该年代下的所有年份，确保元素已加载
                try:
                    year_elements = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, 'div.issue-details-past-tabs.year li a')
                        )
                    )
                    year_names = [elem.text.strip() for elem in year_elements if elem.text.strip()]
                    logging.info(f"年代 {decade_name} 包含年份: {year_names}")

                    # 保存年代数据到JSON缓存
                    self._save_json({'years': year_names}, decade_cache_file)
                except Exception as e:
                    logging.error(f"获取年代 {decade_name} 的年份列表失败: {str(e)}")
                    continue

            # 遍历每个年份
            for year_name in tqdm(year_names, desc=f"处理年份 ({decade_name})", leave=False):
                logging.info(f"处理年份: {year_name}")
                year_cache_file = f'json_cache/years/year_{year_name}.json'

                # 检查年份缓存是否存在
                if self.use_cache and os.path.exists(year_cache_file) and not force_refresh:
                    # 从缓存加载年份数据
                    year_data = self._load_cached_json(year_cache_file)
                    if year_data and 'isnumbers' in year_data:
                        # 将缓存中的isnumber添加到集合
                        for isnumber in year_data['isnumbers']:
                            if isnumber not in self.all_isnumbers:
                                self.all_isnumbers.add(isnumber)
                        logging.info(f"从缓存加载年份 {year_name} 的isnumber数据")
                        continue

                # 点击年份标签，等待期号容器出现
                year_locator = (By.XPATH,
                                f'//div[contains(@class, "issue-details-past-tabs") and contains(@class, "year")]//a[text()="{year_name}"]'
                                )

                # 等待条件：期号容器出现且包含内容
                issue_container_condition = EC.presence_of_element_located(issue_container_selector)

                if not self._click_and_wait(year_locator, issue_container_condition):
                    logging.error(f"无法点击年份标签 {year_name}，跳过该年份")
                    continue

                # 额外等待期号内容加载完成
                time.sleep(random.uniform(2, 4))

                # 从页面获取soup
                year_soup = BeautifulSoup(self.driver.page_source, 'html.parser')

                # 提取该年份下的所有期号
                issue_links = year_soup.select('div.issue-details a[href*="isnumber="]')

                logging.info(f"年份 {year_name} 找到 {len(issue_links)} 个可能的issue链接")

                if not issue_links:
                    logging.warning(f"年份 {year_name} 未找到任何期号链接，检查页面结构")
                    # 尝试其他选择器
                    issue_links = year_soup.select('a[href*="isnumber="]')
                    logging.info(f"尝试备用选择器找到 {len(issue_links)} 个可能的issue链接")

                year_isnumbers = []
                for link in issue_links:
                    href = link.get('href')
                    if not href:
                        continue

                    parsed_url = urlparse(href)
                    query_params = parse_qs(parsed_url.query)

                    if 'isnumber' in query_params and 'punumber' in query_params:
                        if str(query_params['punumber'][0]) == str(self.punumber):
                            isnumber = query_params['isnumber'][0]
                            if isnumber and isnumber not in self.all_isnumbers:
                                self.all_isnumbers.add(isnumber)
                                year_isnumbers.append(isnumber)
                                logging.debug(f"提取到isnumber: {isnumber} (年代: {decade_name}, 年份: {year_name})")

                # 保存年份的isnumber数据到JSON缓存
                self._save_json({'isnumbers': year_isnumbers}, year_cache_file)

        # 转换为排序后的列表
        sorted_isnumbers = sorted(list(self.all_isnumbers))
        logging.info(f"\n===== 提取完成，共获取 {len(sorted_isnumbers)} 个期的isnumber =====")

        # 保存整体isnumber缓存
        self._save_json(sorted_isnumbers, isnumbers_cache_file)

        if not sorted_isnumbers:
            logging.warning("未获取到任何期的isnumber，使用测试数据")
            sorted_isnumbers = ['11192800', '11163533', '11118328']

        return sorted_isnumbers

    def get_paper_links_from_issue(self, isnumber, force_refresh=False):
        """从特定期获取所有论文ID，优化提取逻辑并处理分页"""
        logging.info(f"获取期 isnumber={isnumber} 的论文ID...")
        issue_url = f"{self.base_url}/xpl/tocresult.jsp?isnumber={isnumber}&punumber={self.punumber}"
        cache_file = f'json_cache/issues/issue_{isnumber}.json'

        # 检查缓存
        if self.use_cache and os.path.exists(cache_file) and not force_refresh:
            cached_data = self._load_cached_json(cache_file)
            if cached_data and 'paper_ids' in cached_data:  # 改为存储paper_ids
                return cached_data['paper_ids']

        # 等待论文列表加载的特定元素
        wait_for = (By.CSS_SELECTOR, 'div.List-results-items')

        # 获取期页面
        soup = self._get_soup(issue_url, wait_for=wait_for)
        if not soup:
            return []

        # 提取总论文数量
        total_papers = 0
        try:
            total_results_element = self.driver.find_element(
                By.CSS_SELECTOR, '.Dashboard-header span.strong:last-child'
            )
            if total_results_element:
                total_papers = int(total_results_element.text.strip())
                logging.info(f"期 {isnumber} 总论文数量应为: {total_papers}")
        except Exception as e:
            logging.warning(f"无法获取总论文数量: {str(e)}")
            try:
                total_results_text = self.driver.find_element(By.CSS_SELECTOR, '.results-count').text
                import re
                match = re.search(r'of (\d+)', total_results_text)
                if match:
                    total_papers = int(match.group(1))
                    logging.info(f"期 {isnumber} 总论文数量应为: {total_papers}")
            except Exception as e2:
                logging.warning(f"备选方案也无法获取总论文数量: {str(e2)}")

        # 收集所有页面的论文ID
        all_paper_ids = []
        current_page = 1
        max_pages = 10  # 防止无限循环的安全限制
        last_paper_count = -1  # 用于检测是否有新论文加载

        while current_page <= max_pages:
            logging.info(f"处理第 {current_page} 页的论文ID")

            # 从当前页面提取论文ID
            paper_ids = self._extract_paper_ids_from_page()
            new_ids_count = 0

            for paper_id in paper_ids:
                if paper_id not in all_paper_ids:
                    all_paper_ids.append(paper_id)
                    new_ids_count += 1

            logging.info(f"第 {current_page} 页提取到 {new_ids_count} 个新论文ID")

            # 检查是否已获取所有论文或没有新论文加载
            if (total_papers > 0 and len(all_paper_ids) >= total_papers) or new_ids_count == 0:
                if new_ids_count == 0:
                    logging.info("当前页未提取到新论文ID，停止翻页")
                else:
                    logging.info(f"已获取所有 {total_papers} 篇论文ID")
                break

            # 尝试点击下一页
            if not self._go_to_next_page(current_page):
                logging.info("没有更多页面可浏览")
                break

            current_page += 1
            time.sleep(random.uniform(2, 4))  # 等待页面加载

        # 保存论文ID到JSON缓存
        self._save_json({'paper_ids': all_paper_ids}, cache_file)

        logging.info(f"期 isnumber={isnumber} 共提取 {len(all_paper_ids)} 篇论文ID")
        return all_paper_ids

    def _extract_paper_ids_from_page(self):
        """从当前页面提取论文ID，过滤掉无关链接"""
        paper_ids = []

        # 更精确的选择器，匹配主要论文链接
        selectors = [
            'xpl-issue-results-items h2 a[href*="/document/"]',
            'div.List-results-items h3 a[href*="/document/"]',
            'a.result-item-title[href*="/document/"]'
        ]

        doc_links = []
        for selector in selectors:
            try:
                found_links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if found_links:
                    doc_links.extend(found_links)
                    logging.info(f"使用选择器 {selector} 找到 {len(found_links)} 个论文链接")
            except Exception as e:
                logging.warning(f"使用选择器 {selector} 查找链接时出错: {str(e)}")

        # 处理找到的链接，提取ID并过滤
        for link in doc_links:
            try:
                href = link.get_attribute('href')
                if not href:
                    continue

                # 过滤掉包含引用或媒体的链接
                if '/document/' in href and \
                        '/citations' not in href and \
                        '/media' not in href and \
                        'javascript:' not in href:

                    # 提取论文ID
                    # 从类似 "/document/9858006/" 的链接中提取 "9858006"
                    parts = href.split('/document/')
                    if len(parts) > 1:
                        paper_id = parts[1].split('/')[0]
                        if paper_id and paper_id.isdigit() and paper_id not in paper_ids:
                            paper_ids.append(paper_id)
            except StaleElementReferenceException:
                logging.warning("元素已过期，跳过该链接")
            except Exception as e:
                logging.warning(f"处理链接时出错: {str(e)}")

        return paper_ids

    def _go_to_next_page(self, current_page):
        """点击下一页按钮，根据页码动态调整选择器，返回是否成功"""
        # 尝试多种选择器定位下一页按钮，增加容错性
        selectors = [
            # 基于aria-label的通用选择器
            'button[aria-label="Next page of search results"]',
            # 基于类名前缀的选择器
            'button[class^="stats-Pagination_arrow_next_"]',
            # 基于父元素和类名的选择器
            'li.next-btn button[aria-label="Next page of search results"]'
        ]

        # 尝试通过页码直接点击（备选方案）
        page_number_locator = (By.CSS_SELECTOR, f'button[aria-label="Page {current_page + 1} of search results"]')

        # 先尝试直接点击页码按钮
        try:
            logging.info(f"尝试直接点击第 {current_page + 1} 页按钮")
            return self._click_and_wait(
                page_number_locator,
                wait_condition=EC.presence_of_element_located((By.CSS_SELECTOR, 'div.List-results-items'))
            )
        except Exception as e:
            logging.warning(f"直接点击页码按钮失败: {str(e)}")

        # 尝试各种下一页按钮选择器
        for selector in selectors:
            try:
                next_page_locator = (By.CSS_SELECTOR, selector)

                # 检查按钮是否存在
                next_button = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(next_page_locator)
                )

                # 检查按钮是否可见且可用
                if not next_button.is_displayed() or next_button.get_attribute('disabled'):
                    continue

                # 点击下一页
                logging.info(f"使用选择器 '{selector}' 点击下一页")
                return self._click_and_wait(
                    next_page_locator,
                    wait_condition=EC.presence_of_element_located((By.CSS_SELECTOR, 'div.List-results-items'))
                )
            except NoSuchElementException:
                logging.info(f"未找到匹配选择器 '{selector}' 的下一页按钮")
                continue
            except TimeoutException:
                logging.info(f"等待选择器 '{selector}' 的下一页按钮超时")
                continue
            except Exception as e:
                logging.error(f"使用选择器 '{selector}' 点击下一页时出错: {str(e)}")
                continue

        # 如果所有方法都失败，尝试通过页面URL直接跳转
        try:
            current_url = self.driver.current_url
            if 'page=' in current_url:
                new_url = current_url.replace(f'page={current_page}', f'page={current_page + 1}')
            else:
                separator = '&' if '?' in current_url else '?'
                new_url = f'{current_url}{separator}page={current_page + 1}'

            logging.info(f"尝试通过URL直接跳转至第 {current_page + 1} 页: {new_url}")
            self.driver.get(new_url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.List-results-items'))
            )
            time.sleep(random.uniform(2, 4))
            return True
        except Exception as e:
            logging.error(f"通过URL跳转下一页失败: {str(e)}")
            return False

    def get_paper_details(self, paper_id, force_refresh=False):
        """根据论文ID获取单篇论文的详细信息，并保存JSON缓存"""
        logging.info(f"获取论文详情: ID={paper_id}")
        paper_url = f"{self.base_url}/document/{paper_id}/"
        cache_file = f'json_cache/papers/paper_{paper_id}.json'

        # 检查缓存
        if self.use_cache and os.path.exists(cache_file) and not force_refresh:
            cached_data = self._load_cached_json(cache_file)
            if cached_data:
                return cached_data

        # 等待论文标题元素加载完成（使用更精确的选择器）
        wait_for = (By.CSS_SELECTOR, 'h1.document-title span')

        # 获取页面内容
        soup = self._get_soup(paper_url, wait_for=wait_for)
        if not soup:
            return None

        try:
            # 提取标题 - 使用更精确的选择器组合
            # 主选择器：匹配h1标题内的span
            title_tag = soup.select_one('h1.document-title span')

            # 备选选择器1：使用ngcontent属性但结合h1
            if not title_tag:
                title_tag = soup.select_one('h1[_ngcontent-ng-c729932216] span[_ngcontent-ng-c729932216]')

            # 备选选择器2：仅使用class
            if not title_tag:
                title_tag = soup.select_one('.document-title span')

            # 如果找到标题标签则提取文本，否则标记为无标题
            if title_tag:
                title = title_tag.get_text(strip=True)
                # 过滤无效标题
                if "Journals & Magazines" in title or len(title) < 5:
                    logging.warning(f"可能的无效标题: {title}，尝试其他方式提取")
                    # 尝试从页面元数据中提取标题
                    meta_title = soup.select_one('meta[property="og:title"]')
                    if meta_title:
                        title = meta_title.get('content', '').strip()
            else:
                title = "No title"

            logging.info(f"提取到标题: {title}")

            # 提取作者
            authors = []
            author_tags = soup.select('div.authors-container.stats-document-authors-banner-authorsContainer a')

            for author in author_tags:
                author_name = author.get_text(strip=True)
                if author_name and author_name not in authors and len(author_name) > 1:
                    authors.append(author_name)

            # 提取摘要
            abstract = "No abstract"
            abstract_tag = soup.select_one('div[_ngcontent-ng-c4049499152][xplmathjax][xplreadinglenshighlight]')
            if abstract_tag:
                abstract = abstract_tag.get_text(strip=True)
            else:
                # 尝试点击Abstract按钮获取摘要
                try:
                    abstract_button = self.driver.find_element(
                        By.CSS_SELECTOR, 'button.abstract-control'
                    )
                    if abstract_button:
                        self.driver.execute_script("arguments[0].click();", abstract_button)
                        time.sleep(1)  # 等待摘要展开

                        # 重新获取页面内容
                        updated_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                        abstract_tag = updated_soup.select_one(
                            'div[_ngcontent-ng-c4049499152][xplmathjax][xplreadinglenshighlight]')
                        if abstract_tag:
                            abstract = abstract_tag.get_text(strip=True)
                except Exception as e:
                    logging.warning(f"提取摘要时出错: {str(e)}")

            # 提取引用量
            citations = "No citation data"
            citation_tag = soup.select_one('div[_ngcontent-ng-c729932216].document-banner-metric-count')
            if citation_tag:
                citations = citation_tag.get_text(strip=True)

            # 提取发表日期
            publication_date = "No publication date"
            date_tag = soup.select_one('div[_ngcontent-ng-c4049499152].u-pb-1.doc-abstract-pubdate')
            if date_tag:
                date_text = date_tag.get_text(strip=True)
                # 提取日期部分
                date_match = date_text.replace('Date of Publication:', '').strip()
                if date_match:
                    publication_date = date_match

            # 提取DOI
            doi = "No DOI"
            doi_tag = soup.select_one(
                'div[_ngcontent-ng-c4049499152][data-analytics_identifier="document_abstract_doi"] a')
            if doi_tag:
                doi = doi_tag.get_text(strip=True)

            # 提取PubMed ID
            pubmed_id = "No PubMed ID"
            pubmed_tag = soup.select_one('div[_ngcontent-ng-c4049499152].u-pb-1:has(strong:contains("PubMed ID:")) a')
            if pubmed_tag:
                pubmed_id = pubmed_tag.get_text(strip=True)
            else:
                # 备选方案
                pubmed_div = soup.select_one('div[_ngcontent-ng-c4049499152].u-pb-1:has(strong:contains("PubMed ID:"))')
                if pubmed_div:
                    pubmed_text = pubmed_div.get_text(strip=True)
                    pubmed_id = pubmed_text.replace('PubMed ID:', '').strip()

            paper_info = {
                'id': paper_id,
                'title': title,
                'authors': authors,
                'abstract': abstract,
                'citations': citations,
                'publication_date': publication_date,
                'doi': doi,
                'pubmed_id': pubmed_id,
                'url': paper_url
            }

            # 保存论文详情到JSON缓存
            self._save_json(paper_info, cache_file)

            return paper_info
        except Exception as e:
            logging.error(f"解析论文 ID={paper_id} 失败: {str(e)}")
            return None

    def run(self, max_decades=None, max_years_per_decade=None, max_issues=None, max_papers_per_issue=None,
            force_refresh=False):
        logging.info("开始爬取IEEE期刊论文数据...")

        isnumbers = self.get_all_isnumbers(force_refresh=force_refresh)
        if max_issues:
            isnumbers = isnumbers[:max_issues]

        for isnumber in tqdm(isnumbers, desc="处理期刊期数"):
            paper_ids = self.get_paper_links_from_issue(isnumber, force_refresh=force_refresh)
            if max_papers_per_issue:
                paper_ids = paper_ids[:max_papers_per_issue]

            for paper_id in tqdm(paper_ids, desc=f"处理论文 (期: {isnumber})", leave=False):
                paper_details = self.get_paper_details(paper_id, force_refresh=force_refresh)
                if paper_details:
                    self.all_papers.append(paper_details)

        logging.info(f"爬取完成，共获取 {len(self.all_papers)} 篇论文数据")
        # 关闭浏览器
        self.driver.quit()
        return self.all_papers

    def save_to_json(self, filename="ieee_papers.json"):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_papers, f, ensure_ascii=False, indent=4)
            logging.info(f"数据已成功保存到 {filename}")
        except Exception as e:
            logging.error(f"保存JSON文件失败: {str(e)}")


if __name__ == "__main__":
    # 爬取punumber=34的期刊，可通过use_cache=False禁用缓存
    scraper = IEEEScraper(punumber=34, use_cache=True)

    # force_refresh=True 可强制刷新缓存
    scraper.run(
        force_refresh=False
    )

    scraper.save_to_json()

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

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

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
        self.driver.set_page_load_timeout(60)  # 页面加载超时时间

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

        # 如果所有方法都失败，尝试通过URL直接跳转
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

    def _click_keywords_accordion(self):
        """专门点击关键词折叠面板，确保内容显示"""
        try:
            # 定义关键词按钮的多种定位器（增加容错性）
            button_locators = [
                (By.ID, 'keywords'),  # 最直接的ID定位
                (By.CSS_SELECTOR, 'button#keywords'),  # ID+标签组合
                (By.XPATH, '//button[contains(@aria-controls, "keywords") and .//div[text()="Keywords"]]'),  # 功能定位
                (By.CSS_SELECTOR, 'button[aria-controls="keywords"][aria-expanded]')  # 属性组合定位
            ]

            clicked = False
            for locator in button_locators:
                try:
                    # 等待按钮出现
                    keyword_button = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located(locator)
                    )

                    # 滚动到按钮位置（确保可见）
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                                               keyword_button)
                    time.sleep(2)  # 等待滚动完成

                    # 检查当前状态，如果已经展开则不需要点击
                    current_state = keyword_button.get_attribute('aria-expanded')
                    if current_state == 'true':
                        logging.info(f"关键词面板已展开（定位器: {locator}）")
                        clicked = True
                        break

                    # 检查是否被其他元素遮挡
                    try:
                        # 尝试直接点击
                        keyword_button.click()
                        logging.info(f"成功点击关键词按钮（定位器: {locator}）")
                        clicked = True
                        break
                    except ElementClickInterceptedException:
                        # 被遮挡，使用JavaScript点击
                        self.driver.execute_script("arguments[0].click();", keyword_button)
                        logging.info(f"使用JavaScript点击关键词按钮（定位器: {locator}）")
                        clicked = True
                        break
                except TimeoutException:
                    logging.info(f"未找到关键词按钮（定位器: {locator}），尝试下一个")
                    continue
                except Exception as e:
                    logging.warning(f"点击关键词按钮失败（定位器: {locator}）: {str(e)}")
                    continue

            if not clicked:
                logging.error("所有定位器都无法找到/点击关键词按钮")
                return False

            # 点击后等待内容加载（关键！）
            time.sleep(3)  # 等待折叠面板展开动画

            # 验证内容是否已加载
            try:
                # 等待关键词容器出现
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.stats-keywords-container'))
                )
                logging.info("关键词面板展开并加载完成")
                return True
            except TimeoutException:
                logging.warning("关键词面板点击后，内容未及时加载")
                # 再次尝试点击（有时候需要点击两次）
                try:
                    keyword_button = self.driver.find_element(By.ID, 'keywords')
                    self.driver.execute_script("arguments[0].click();", keyword_button)
                    logging.info("再次点击关键词按钮")
                    time.sleep(3)
                    # 再次验证
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.stats-keywords-container'))
                    )
                    logging.info("关键词面板第二次点击后加载完成")
                    return True
                except Exception as e:
                    logging.error(f"第二次点击后仍未加载关键词内容: {str(e)}")
                    return False

        except Exception as e:
            logging.error(f"处理关键词折叠面板时发生严重错误: {str(e)}")
            return False

    def _click_authors_accordion(self):
        """专门点击作者折叠面板，确保内容显示"""
        try:
            # 定义作者按钮的多种定位器（仿照关键词按钮的实现）
            button_locators = [
                (By.ID, 'authors'),  # 最直接的ID定位
                (By.CSS_SELECTOR, 'button#authors'),  # ID+标签组合
                (By.XPATH, '//button[contains(@aria-controls, "authors") and .//div[text()="Authors"]]'),  # 功能定位
                (By.CSS_SELECTOR, 'button[aria-controls="authors"][aria-expanded]')  # 属性组合定位
            ]

            clicked = False
            for locator in button_locators:
                try:
                    # 等待按钮出现
                    author_button = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_element_located(locator)
                    )

                    # 滚动到按钮位置（确保可见）
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                                               author_button)
                    time.sleep(2)  # 等待滚动完成

                    # 检查当前状态，如果已经展开则不需要点击
                    current_state = author_button.get_attribute('aria-expanded')
                    if current_state == 'true':
                        logging.info(f"作者面板已展开（定位器: {locator}）")
                        clicked = True
                        break

                    # 检查是否被其他元素遮挡
                    try:
                        # 尝试直接点击
                        author_button.click()
                        logging.info(f"成功点击作者按钮（定位器: {locator}）")
                        clicked = True
                        break
                    except ElementClickInterceptedException:
                        # 被遮挡，使用JavaScript点击
                        self.driver.execute_script("arguments[0].click();", author_button)
                        logging.info(f"使用JavaScript点击作者按钮（定位器: {locator}）")
                        clicked = True
                        break
                except TimeoutException:
                    logging.info(f"未找到作者按钮（定位器: {locator}），尝试下一个")
                    continue
                except Exception as e:
                    logging.warning(f"点击作者按钮失败（定位器: {locator}）: {str(e)}")
                    continue

            if not clicked:
                logging.error("所有定位器都无法找到/点击作者按钮")
                return False

            # 点击后等待内容加载（关键！）
            time.sleep(3)  # 等待折叠面板展开动画

            # 验证内容是否已加载
            try:
                # 等待作者容器出现
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.authors-accordion-container'))
                )
                logging.info("作者面板展开并加载完成")
                return True
            except TimeoutException:
                logging.warning("作者面板点击后，内容未及时加载")
                # 再次尝试点击（有时候需要点击两次）
                try:
                    author_button = self.driver.find_element(By.ID, 'authors')
                    self.driver.execute_script("arguments[0].click();", author_button)
                    logging.info("再次点击作者按钮")
                    time.sleep(3)
                    # 再次验证
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.authors-accordion-container'))
                    )
                    logging.info("作者面板第二次点击后加载完成")
                    return True
                except Exception as e:
                    logging.error(f"第二次点击后仍未加载作者内容: {str(e)}")
                    return False

        except Exception as e:
            logging.error(f"处理作者折叠面板时发生严重错误: {str(e)}")
            return False

    def _extract_keywords(self, soup):
        """提取三种类型的关键词：IEEE Keywords、Index Terms、Author Keywords - 最终优化版"""
        keywords = {
            'ieee_keywords': [],
            'index_terms': [],
            'author_keywords': []
        }

        # 定义关键词类型映射
        keyword_mapping = {
            'ieee_keywords': 'IEEE Keywords',
            'index_terms': 'Index Terms',
            'author_keywords': 'Author Keywords'
        }

        # 第一步：找到关键词容器（基于用户提供的HTML结构）
        # 尝试多种容器选择器，确保能找到（新增更多备选选择器）
        container_selectors = [
            'div.stats-keywords-container',  # 直接容器
            'xpl-document-keyword-list section.keywords-tab',  # 父容器
            'ul.doc-keywords-list.stats-keywords-list',  # 列表容器
            'div.accordion-body#keywords div.stats-keywords-container',  # 完整路径
            'div[_ngcontent-ng-c4006922076].accordion-body div.stats-keywords-container',  # 包含ng-content的容器
            'div.accordion-item#keywords-item div.accordion-body',  # accordion-item父容器
            'section.keywords-tab',  # 简化的父容器
            'div.doc-keywords'  # 通用关键词容器
        ]

        keywords_container = None
        for selector in container_selectors:
            keywords_container = soup.select_one(selector)
            if keywords_container:
                logging.info(f"使用选择器找到关键词容器: {selector}")
                break

        if not keywords_container:
            logging.error("未找到任何关键词容器，返回空关键词")
            # 尝试直接从整个页面查找关键词相关元素（增强容错）
            try:
                # 直接查找所有关键词列表项（不限制父容器）
                keyword_items = soup.select('li.doc-keywords-list-item')
                if keyword_items:
                    keywords_container = BeautifulSoup('<div></div>', 'html.parser')
                    for item in keyword_items:
                        keywords_container.div.append(item)
                    logging.info(f"通过直接查找关键词列表项找到 {len(keyword_items)} 个关键词项")
                else:
                    # 尝试查找包含关键词标签的div
                    keyword_divs = soup.select(
                        'div:has(strong:contains("IEEE Keywords"), strong:contains("Index Terms"), strong:contains("Author Keywords"))')
                    if keyword_divs:
                        keywords_container = keyword_divs[0]
                        logging.info(f"通过关键词标签找到容器: {keyword_divs[0].name}")
            except Exception as e:
                logging.warning(f"直接查找关键词列表项失败: {str(e)}")
            if not keywords_container:
                return keywords

        # 第二步：遍历每种关键词类型，精确提取
        for key, label in keyword_mapping.items():
            try:
                # 方法1：先找包含该标签的li元素（最精确）
                keyword_li = keywords_container.find('li', class_='doc-keywords-list-item',
                                                     string=lambda text: text and label in text)

                if not keyword_li:
                    # 方法2：直接找strong标签，再找父li
                    strong_tag = keywords_container.find('strong', string=label)
                    if strong_tag:
                        keyword_li = strong_tag.find_parent('li', class_='doc-keywords-list-item')

                if not keyword_li:
                    # 方法3：不限制父容器，直接在整个soup中查找
                    strong_tag = soup.find('strong', string=label)
                    if strong_tag:
                        keyword_li = strong_tag.find_parent('li', class_='doc-keywords-list-item')

                if not keyword_li and strong_tag:
                    # 方法4：如果找到strong标签但没有li父元素，直接用strong的父元素
                    keyword_li = strong_tag.find_parent()

                if keyword_li:
                    # 提取所有关键词链接
                    # 尝试多种链接选择器
                    link_selectors = [
                        'a.stats-keywords-list-item',
                        'ul.List--inline li a',
                        'ul.u-mt-1 a',
                        'a[href*="/search/searchresult.jsp?matchBoolean=true&amp;queryText="]',
                        'a[href*="/search/searchresult.jsp?queryText="]',
                        'a.doc-keyword-link',  # 新增关键词链接类名
                        'a[class*="keyword"]'  # 模糊匹配关键词链接
                    ]

                    keyword_links = []
                    for link_selector in link_selectors:
                        keyword_links = keyword_li.select(link_selector)
                        if keyword_links:
                            logging.debug(
                                f"关键词类型 {label} 使用选择器 {link_selector} 找到 {len(keyword_links)} 个链接")
                            break

                    # 如果没有找到链接，直接提取文本
                    if not keyword_links:
                        try:
                            # 提取li元素中的所有文本，排除strong标签
                            all_text = keyword_li.get_text(separator=',', strip=True)
                            # 移除标签文本
                            all_text = all_text.replace(label, '').strip()
                            if all_text:
                                # 按逗号分割
                                keyword_list = [kw.strip() for kw in all_text.split(',') if
                                                kw.strip() and len(kw.strip()) > 1]
                                keywords[key] = keyword_list
                                logging.info(f"直接提取文本获取 {label}: {len(keyword_list)} 个关键词")
                                continue
                        except Exception as e:
                            logging.warning(f"直接提取关键词文本失败: {str(e)}")

                    # 提取关键词文本
                    keyword_list = []
                    for link in keyword_links:
                        keyword_text = link.get_text(strip=True)
                        if keyword_text:
                            # 清理关键词（移除末尾逗号、空格等）
                            cleaned_keyword = keyword_text.rstrip(',').rstrip('.').strip()
                            # 过滤无效关键词
                            if cleaned_keyword and len(cleaned_keyword) > 1 and cleaned_keyword not in keyword_list:
                                keyword_list.append(cleaned_keyword)

                    keywords[key] = keyword_list
                    logging.info(f"成功提取 {label}: {len(keyword_list)} 个关键词 - {keyword_list[:5]}...")  # 只显示前5个
                else:
                    logging.debug(f"未找到 {label} 对应的元素")
                    keywords[key] = []

            except Exception as e:
                logging.error(f"提取 {label} 时出错: {str(e)}", exc_info=True)
                keywords[key] = []

        # 验证是否提取到关键词
        total_keywords = sum(len(v) for v in keywords.values())
        if total_keywords == 0:
            logging.warning("所有关键词类型都未提取到数据，尝试终极方案：直接提取文本")
            try:
                # 终极方案：直接提取所有文本，按标签分割
                all_text = keywords_container.get_text(separator='|', strip=True)
                # 按关键词标签分割
                for key, label in keyword_mapping.items():
                    if label in all_text:
                        # 提取标签后的内容，直到下一个标签或结束
                        parts = all_text.split(label)[1:]
                        if parts:
                            # 找到下一个标签的位置
                            next_label_pos = len(parts[0])
                            for next_label in keyword_mapping.values():
                                if next_label != label and next_label in parts[0]:
                                    next_label_pos = min(next_label_pos, parts[0].index(next_label))
                            # 提取关键词部分
                            keywords_part = parts[0][:next_label_pos]
                            # 按逗号分割
                            keyword_list = [kw.strip() for kw in keywords_part.split(',') if
                                            kw.strip() and len(kw.strip()) > 1]
                            keywords[key] = keyword_list
                            logging.info(f"终极方案提取 {label}: {len(keyword_list)} 个关键词")
            except Exception as e:
                logging.error(f"终极方案提取关键词失败: {str(e)}")

        return keywords

    def _extract_author_details(self, soup):
        """提取作者机构和作者简介"""
        author_details = {
            'author_affiliations': [],  # 作者机构列表（与authors顺序对应）
            'author_bios': []  # 作者简介列表（与authors顺序对应）
        }

        try:
            # 找到所有作者卡片容器（使用更通用的选择器）
            author_items = soup.select('xpl-author-item, div.author-card')
            logging.info(f"找到 {len(author_items)} 个作者卡片")

            for author_item in author_items:
                # 提取机构信息
                affiliations = []
                # 查找作者机构的div（多种可能的选择器）
                affiliation_selectors = [
                    'div[_ngcontent-ng-c2256705020].col-14-24 div[_ngcontent-ng-c2256705020]:not(:has(a))',
                    'div.col-14-24 div:not(:has(a))',
                    'div.author-affiliation',
                    'div[_ngcontent-ng-c2256705020]:-soup-contains(",")',  # 修复：:contains → :-soup-contains
                    'div.affiliation-text',  # 新增机构文本选择器
                    'div.institution'  # 新增机构选择器
                ]

                for selector in affiliation_selectors:
                    affiliation_divs = author_item.select(selector)
                    if affiliation_divs:
                        for div in affiliation_divs:
                            aff_text = div.get_text(strip=True)
                            # 过滤掉太短或明显不是机构的文本
                            if aff_text and len(aff_text) > 5 and aff_text not in affiliations and \
                                    not any(
                                        word in aff_text.lower() for word in ['received', 'degree', 'phd', 'be', 'me']):
                                affiliations.append(aff_text)
                        if affiliations:
                            break

                # 提取作者简介
                bio_text = ""
                # 查找作者简介的span标签（多种可能的选择器）
                bio_selectors = [
                    'xpl-author-bio[_nghost-ng-c2723203832] span[_ngcontent-ng-c2723203832]',
                    'xpl-author-bio span',
                    'div.author-bio span',
                    'span[_ngcontent-ng-c2723203832]',
                    'div.bio-text',  # 新增简介文本选择器
                    'p.author-bio'  # 新增简介选择器
                ]

                for selector in bio_selectors:
                    bio_spans = author_item.select(selector)
                    if bio_spans:
                        # 取第一个非空的简介文本
                        for span in bio_spans:
                            bio_text = span.get_text(strip=True)
                            if bio_text and len(bio_text) > 20:  # 简介通常较长
                                break
                        if bio_text:
                            break

                # 添加到结果列表
                author_details['author_affiliations'].append(affiliations if affiliations else [])
                author_details['author_bios'].append(bio_text if bio_text else "")

            # 确保作者机构和简介列表与作者列表长度一致（如果不一致，补充空值）
            authors_count = len(soup.select('div.authors-container a') or soup.select('xpl-author-item a'))
            while len(author_details['author_affiliations']) < authors_count:
                author_details['author_affiliations'].append([])
            while len(author_details['author_bios']) < authors_count:
                author_details['author_bios'].append("")

            logging.info(f"提取完成：{len(author_details['author_affiliations'])} 个作者的机构和简介")
        except Exception as e:
            logging.error(f"提取作者机构和简介时出错: {str(e)}", exc_info=True)
            # 保持返回结构一致
            author_details['author_affiliations'] = []
            author_details['author_bios'] = []

        return author_details

    def get_paper_details(self, paper_id, force_refresh=False):
        """根据论文ID获取单篇论文的详细信息，并保存JSON缓存"""
        logging.info(f"\n===== 开始获取论文详情: ID={paper_id} =====")
        paper_url = f"{self.base_url}/document/{paper_id}/"
        cache_file = f'json_cache/papers/paper_{paper_id}.json'

        # 检查缓存
        if self.use_cache and os.path.exists(cache_file) and not force_refresh:
            cached_data = self._load_cached_json(cache_file)
            if cached_data:
                # 检查是否已有关键词和作者详情数据
                has_keywords = any(
                    len(cached_data.get(key, [])) > 0 for key in ['ieee_keywords', 'index_terms', 'author_keywords'])
                has_author_details = 'author_affiliations' in cached_data and 'author_bios' in cached_data
                if has_keywords and has_author_details:
                    logging.info(f"从缓存加载论文 {paper_id}（含关键词和作者详情数据）")
                    return cached_data
                else:
                    logging.info(f"缓存中论文 {paper_id} 数据不完整，重新获取")

        # 等待论文标题元素加载完成
        wait_for = (By.CSS_SELECTOR, 'h1.document-title span')

        # 获取页面内容（只获取一次页面，后续通过刷新源码获取动态内容）
        self._get_soup(paper_url, wait_for=wait_for)
        if not self.driver.page_source.strip():
            logging.error(f"无法获取论文页面: {paper_url}")
            return None

        try:
            # ========== 关键修改：分步提取，避免面板互斥 ==========
            # 1. 提取基础信息（标题、作者、摘要等，不需要展开面板的信息）
            base_soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # 提取标题
            title_tag = base_soup.select_one('h1.document-title span') or \
                        base_soup.select_one('h1[_ngcontent-ng-c729932216] span[_ngcontent-ng-c729932216]') or \
                        base_soup.select_one('.document-title span')
            title = title_tag.get_text(strip=True) if title_tag else "No title"
            logging.info(f"论文标题: {title[:50]}...")  # 只显示前50个字符

            # 提取作者（基础作者名，不需要展开面板）
            authors = []
            author_tags = base_soup.select('div.authors-container.stats-document-authors-banner-authorsContainer a')
            # 如果没找到，尝试其他选择器
            if not author_tags:
                author_tags = base_soup.select('xpl-author-item a, div.author-card a')
            for author in author_tags:
                author_name = author.get_text(strip=True)
                if author_name and author_name not in authors and len(author_name) > 1:
                    authors.append(author_name)
            logging.info(f"作者: {authors[:3]}...")  # 只显示前3个作者

            # 提取摘要
            abstract = "No abstract"
            abstract_tag = base_soup.select_one('div[_ngcontent-ng-c4049499152][xplmathjax][xplreadinglenshighlight]')
            if not abstract_tag:
                try:
                    abstract_button = self.driver.find_element(By.CSS_SELECTOR, 'button.abstract-control')
                    if abstract_button:
                        self.driver.execute_script("arguments[0].click();", abstract_button)
                        time.sleep(2)
                        abstract_tag = BeautifulSoup(self.driver.page_source, 'html.parser').select_one(
                            'div[_ngcontent-ng-c4049499152][xplmathjax][xplreadinglenshighlight]')
                except Exception as e:
                    logging.warning(f"提取摘要时出错: {str(e)}")
            if abstract_tag:
                abstract = abstract_tag.get_text(strip=True)
            logging.info(f"摘要长度: {len(abstract)} 字符")

            # 提取引用量
            citations = "No citation data"
            citation_tag = base_soup.select_one('div[_ngcontent-ng-c729932216].document-banner-metric-count')
            if citation_tag:
                citations = citation_tag.get_text(strip=True)

            # 提取发表日期
            publication_date = "No publication date"
            date_tag = base_soup.select_one('div[_ngcontent-ng-c4049499152].u-pb-1.doc-abstract-pubdate')
            if date_tag:
                date_text = date_tag.get_text(strip=True)
                publication_date = date_text.replace('Date of Publication:', '').strip()

            # 提取DOI
            doi = "No DOI"
            doi_tag = base_soup.select_one(
                'div[_ngcontent-ng-c4049499152][data-analytics_identifier="document_abstract_doi"] a')
            if doi_tag:
                doi = doi_tag.get_text(strip=True)

            # 提取PubMed ID
            pubmed_id = "No PubMed ID"
            # 修复：:contains → :-soup-contains
            pubmed_tag = base_soup.select_one(
                'div[_ngcontent-ng-c4049499152].u-pb-1:has(strong:-soup-contains("PubMed ID:")) a')
            if not pubmed_tag:
                # 修复：:contains → :-soup-contains
                pubmed_div = base_soup.select_one(
                    'div[_ngcontent-ng-c4049499152].u-pb-1:has(strong:-soup-contains("PubMed ID:"))')
                if pubmed_div:
                    pubmed_text = pubmed_div.get_text(strip=True)
                    pubmed_id = pubmed_text.replace('PubMed ID:', '').strip()

            # 2. 提取关键词：展开关键词面板 → 提取 → 不关闭（后续会被作者面板顶掉，但已经提取完了）
            logging.info("开始提取关键词...")
            keywords = {}
            if self._click_keywords_accordion():
                # 关键词面板展开后，立即获取当前页面源码
                keywords_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                keywords = self._extract_keywords(keywords_soup)
            else:
                logging.error("关键词面板展开失败，关键词为空")
                keywords = {'ieee_keywords': [], 'index_terms': [], 'author_keywords': []}

            # 3. 提取作者详情：展开作者面板 → 提取（此时关键词面板会自动折叠，但已提取完成）
            logging.info("开始提取作者机构和简介...")
            author_details = {}
            if self._click_authors_accordion():
                # 作者面板展开后，立即获取当前页面源码
                authors_soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                author_details = self._extract_author_details(authors_soup)
            else:
                logging.error("作者面板展开失败，作者详情为空")
                author_details = {'author_affiliations': [], 'author_bios': []}

            # ========== 数据整合 ==========
            # 打印关键词提取结果（方便调试）
            logging.info(f"关键词提取结果:")
            logging.info(f"  IEEE Keywords: {len(keywords['ieee_keywords'])} 个")
            logging.info(f"  Index Terms: {len(keywords['index_terms'])} 个")
            logging.info(f"  Author Keywords: {len(keywords['author_keywords'])} 个")

            # 打印作者详情提取结果
            logging.info(f"作者详情提取结果:")
            logging.info(f"  作者数量: {len(authors)}")
            logging.info(f"  机构信息数量: {len(author_details['author_affiliations'])}")
            logging.info(f"  简介信息数量: {len(author_details['author_bios'])}")

            paper_info = {
                'id': paper_id,
                'title': title,
                'authors': authors,
                'author_affiliations': author_details['author_affiliations'],  # 新增字段：作者机构
                'author_bios': author_details['author_bios'],  # 新增字段：作者简介
                'abstract': abstract,
                'citations': citations,
                'publication_date': publication_date,
                'doi': doi,
                'pubmed_id': pubmed_id,
                'url': paper_url,
                'ieee_keywords': keywords['ieee_keywords'],
                'index_terms': keywords['index_terms'],
                'author_keywords': keywords['author_keywords']
            }

            # 保存论文详情到JSON缓存
            self._save_json(paper_info, cache_file)
            logging.info(f"论文 {paper_id} 详情保存完成")

            return paper_info
        except Exception as e:
            logging.error(f"解析论文 ID={paper_id} 失败: {str(e)}", exc_info=True)
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

        logging.info(f"\n===== 爬取完成，共获取 {len(self.all_papers)} 篇论文数据 =====")
        # 统计关键词提取情况
        papers_with_keywords = sum(1 for p in self.all_papers if any(
            len(p.get(key, [])) > 0 for key in ['ieee_keywords', 'index_terms', 'author_keywords']))
        logging.info(f"其中 {papers_with_keywords} 篇论文成功提取到关键词")

        # 统计作者详情提取情况
        papers_with_author_details = sum(1 for p in self.all_papers if
                                         len(p.get('author_affiliations', [])) > 0 or
                                         len(p.get('author_bios', [])) > 0)
        logging.info(f"其中 {papers_with_author_details} 篇论文成功提取到作者详情")

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
    scraper = IEEEScraper(punumber=34, use_cache=True)

    scraper.run(
        force_refresh=False
    )

    scraper.save_to_json()

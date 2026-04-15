import os
import sqlite3
import logging
import requests
import cloudscraper
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime

# ==========================================
# LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ==========================================
# PHASE 1: DATABASE MANAGER
# ==========================================
class DatabaseManager:
    def __init__(self, db_name: str = "med_alerts.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self._create_tables()

    def _create_tables(self) -> None:
        query = """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_title TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                publication_date TEXT,
                is_sent BOOLEAN DEFAULT 0
            )
        """
        with self.conn:
            self.conn.execute(query)

    def insert_job(self, title: str, link: str, pub_date: str) -> bool:
        query = "INSERT INTO jobs (job_title, link, publication_date) VALUES (?, ?, ?)"
        try:
            with self.conn:
                self.conn.execute(query, (title, link, pub_date))
            return True
        except sqlite3.IntegrityError:
            return False

    def mark_as_sent(self, link: str) -> None:
        """Marks a job as successfully sent to Telegram."""
        query = "UPDATE jobs SET is_sent = 1 WHERE link = ?"
        with self.conn:
            self.conn.execute(query, (link,))

    def close(self):
        self.conn.close()


# ==========================================
# PHASE 3: TELEGRAM NOTIFIER
# ==========================================
class TelegramNotifier:
    """Handles sending notifications via Telegram Bot API."""
    
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text: str) -> bool:
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True # Keeps the chat clean without huge link previews
        }
        try:
            response = requests.post(self.base_url, json=payload, timeout=10)
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logging.error(f"Failed to send Telegram message. Error: {e}")
            return False


# ==========================================
# PHASE 2: SCRAPING ENGINE (UPDATED & RESILIENT)
# ==========================================
class BaseScraper:
    def __init__(self):
        # Cloudscraper já gerencia os headers e simula um navegador real automaticamente
        self.scraper = cloudscraper.create_scraper(browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        })
        self.keywords = ["médico", "medico", "clínico geral", "clinico geral", "saúde da família", "saude da familia", "crm"]
        self.state_filters = ["rj", "rio de janeiro", "macaé", "macae"]

    def fetch_html(self, url: str) -> Optional[str]:
        try:
            logging.info(f"Fetching data from: {url}")
            response = self.scraper.get(url, timeout=15)
            response.raise_for_status()
            
            # 🐛 DEBUG: Descomente as duas linhas abaixo se quiser salvar o HTML para investigar
            # with open("debug_site.html", "w", encoding="utf-8") as f:
            #     f.write(response.text)
                
            return response.text
        except Exception as e:
            logging.error(f"Failed to fetch {url}. Error: {e}")
            return None

    def is_relevant(self, text: str) -> bool:
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.keywords)

    def is_in_target_state(self, text: str) -> bool:
        text_lower = text.lower()
        words = text_lower.replace(",", " ").replace("-", " ").split()
        return any(state in words or state in text_lower for state in self.state_filters)

    def scrape(self) -> List[Dict[str, str]]:
        raise NotImplementedError("Subclasses must implement the scrape() method.")


class PCIScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/sudeste/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        # Abordagem Resiliente: Pega TODOS os links da página
        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")
            
            # Filtro básico para ignorar links de navegação do site (menus, etc)
            if not title or "concursos" not in link_href:
                continue

            # Pega o bloco pai (geralmente uma <li> ou <div>) para ler o contexto todo (Estado, Data, Vagas)
            parent_block = link_element.parent
            if not parent_block:
                continue
                
            block_text = parent_block.text.strip()

            # Passa no nosso filtro rigoroso?
            if self.is_in_target_state(block_text) and self.is_relevant(block_text):
                
                # Tenta achar uma data no texto do bloco com regex simples ou fallback
                pub_date = datetime.now().strftime("%Y-%m-%d")
                if "202" in block_text: # Tenta extrair algo que pareça um ano para compor info (Opcional)
                    pass 
                
                found_jobs.append({
                    "title": title,
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": pub_date
                })

        # Remove duplicatas baseadas no link (caso o mesmo link apareça 2x no HTML)
        unique_jobs = {job['link']: job for job in found_jobs}.values()

        logging.info(f"[PCIScraper] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

class GoogleNewsScraper(BaseScraper):
    """
    Scraper focado no Google News, buscando por termos específicos.
    Isso engloba Folha Dirigida, Estratégia Concursos, Sanar, etc.
    """
    def __init__(self):
        super().__init__()
        # A URL já traz a busca "concurso medico" filtrada para os últimos 7 dias (when:7d)
        self.url = "https://news.google.com/search?q=concurso%20medico%20rj%20OR%20rio%20de%20janeiro%20OR%20macae%20when%3A7d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        # No Google News, as notícias costumam estar em tags <article>
        articles = soup.find_all("article")

        for article in articles:
            link_element = article.find("a")
            if not link_element:
                continue

            title = link_element.text.strip()
            # Os links do Google News começam com "./articles/...", precisamos consertar isso
            raw_link = link_element.get("href", "")
            if raw_link.startswith("./"):
                link_href = f"https://news.google.com{raw_link[1:]}"
            else:
                link_href = raw_link

            article_text = article.text.strip()

            # Como a própria URL de busca já é filtrada, aqui somos um pouco mais flexíveis,
            # mas ainda garantimos que tenha a ver com a área médica.
            if self.is_relevant(article_text):
                found_jobs.append({
                    "title": f"[Notícia/Radar] {title}",
                    "link": link_href,
                    "pub_date": datetime.now().strftime("%Y-%m-%d")
                })

        # Remove duplicatas baseadas no link
        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[GoogleNews] Found {len(unique_jobs)} relevant medical news/jobs.")
        return list(unique_jobs)


class G1Scraper(BaseScraper):
    """Scraper focado na editoria de concursos do G1 (Nacional e Sudeste)."""
    def __init__(self):
        super().__init__()
        self.url = "https://g1.globo.com/trabalho-e-carreira/concursos/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        # O G1 usa a classe 'feed-post-link' para os títulos das notícias na home
        links = soup.find_all("a", class_="feed-post-link")

        for link_element in links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            # Precisamos verificar o título da notícia para saber se é do nosso interesse
            if self.is_in_target_state(title) and self.is_relevant(title):
                found_jobs.append({
                    "title": f"[G1] {title}",
                    "link": link_href,
                    "pub_date": datetime.now().strftime("%Y-%m-%d")
                })

        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[G1Scraper] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

# ==========================================
# PHASE 4: MAIN EXECUTION LOGIC
# ==========================================
if __name__ == "__main__":
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    logging.info("Starting MedAlert RJ Scraper Engine...")

    db = DatabaseManager()
    notifier = TelegramNotifier(bot_token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID)
    scrapers: List[BaseScraper] = [
        PCIScraper(),
        GoogleNewsScraper(),
        G1Scraper()
    ]

    new_jobs_count = 0
    messages_sent = 0

    for scraper in scrapers:
        jobs = scraper.scrape()
        
        for job in jobs:
            is_new = db.insert_job(
                title=job["title"],
                link=job["link"],
                pub_date=job["pub_date"]
            )
            
            if is_new:
                new_jobs_count += 1
                logging.info(f"🆕 NEW JOB SAVED: {job['title']}")
                
                # Format the message for Telegram
                msg = (
                    f"🚨 *Novo Processo Seletivo Encontrado!*\n\n"
                    f"🏥 *Vaga:* {job['title']}\n"
                    f"📅 *Data limite/Info:* {job['pub_date']}\n\n"
                    f"🔗 [Clique aqui para acessar o edital]({job['link']})"
                )
                
                # Send and mark as sent
                if notifier.send_message(msg):
                    db.mark_as_sent(job['link'])
                    messages_sent += 1

    logging.info(f"Execution finished. {new_jobs_count} new jobs added. {messages_sent} Telegram alerts sent.")
    db.close()
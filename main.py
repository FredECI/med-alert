import os
import re
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


import csv

# ==========================================
# PHASE 1.5: REPORT GENERATOR
# ==========================================
class ReportGenerator:
    """Gera relatórios consolidados a partir do banco de dados SQLite."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def fetch_active_jobs(self) -> List[tuple]:
        """Busca todas as vagas no banco de dados, ordenadas pelas mais recentes."""
        query = "SELECT job_title, link, publication_date FROM jobs ORDER BY publication_date DESC, id DESC"
        with self.db.conn:
            cursor = self.db.conn.execute(query)
            return cursor.fetchall()

    def generate_csv(self, filename: str = "vagas_abertas.csv") -> None:
        """Exporta as vagas para uma planilha CSV."""
        jobs = self.fetch_active_jobs()
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';') # Ponto e vírgula é melhor para o Excel em português
            writer.writerow(["Data de Captura", "Título do Processo Seletivo", "Link de Acesso"])
            
            for job in jobs:
                writer.writerow([job[2], job[0], job[1]])
                
        logging.info(f"📊 Relatório CSV gerado: {filename} com {len(jobs)} vagas.")

    def generate_markdown(self, filename: str = "index.md") -> None:
        """Exporta as vagas para um arquivo index.md (Página inicial do site)."""
        jobs = self.fetch_active_jobs()
        
        with open(filename, mode='w', encoding='utf-8') as file:
            # Esse cabeçalho (Frontmatter) diz ao GitHub para usar um layout legal
            file.write("---\n")
            file.write("layout: default\n")
            file.write("title: MedAlert RJ\n")
            file.write("---\n\n")
            
            file.write("# 🩺 MedAlert: Radar de Oportunidades\n\n")
            file.write("Painel atualizado automaticamente com editais e processos seletivos abertos, com foco especial em Macaé, capital e regiões próximas.\n\n")
            
            file.write(f"**Última atualização do robô:** {datetime.now().strftime('%d/%m/%Y às %H:%M')}\n\n")
            
            file.write("| Data de Descoberta | Título do Processo Seletivo | Link Oficial |\n")
            file.write("| :--- | :--- | :--- |\n")
            
            for job in jobs:
                title = job[0].replace("|", "-")
                link = job[1]
                date = job[2]
                file.write(f"| {date} | **{title}** | [Acessar Edital]({link}) |\n")
                
        logging.info(f"📝 Site gerado: {filename} com {len(jobs)} vagas.")


# ==========================================
# PHASE 3: TELEGRAM NOTIFIER
# ==========================================
class TelegramNotifier:
    """Handles sending notifications to multiple Telegram chats."""
    
    def __init__(self, bot_token: str, chat_ids: List[str]):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text: str) -> int:
        """
        Envia a mensagem para todos os chats configurados.
        Retorna a quantidade de mensagens enviadas com sucesso.
        """
        success_count = 0
        
        if not self.chat_ids:
            logging.warning("Nenhum Chat ID configurado para envio.")
            return 0

        for chat_id in self.chat_ids:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            try:
                response = requests.post(self.base_url, json=payload, timeout=10)
                response.raise_for_status()
                success_count += 1
            except requests.RequestException as e:
                logging.error(f"Failed to send Telegram message to {chat_id}. Error: {e}")
                
        return success_count


# ==========================================
# PHASE 2: SCRAPING ENGINE
# ==========================================
class BaseScraper:
    def __init__(self):
        # Cloudscraper já gerencia os headers e simula um navegador real automaticamente
        self.scraper = cloudscraper.create_scraper(browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        })
        self.keywords = [
            "médico", "medico", "clínico geral", "clinico geral", 
            "saúde da família", "saude da familia", "crm", 
            "esf", "psf", "ubs", "upa", "plantão", "plantao", 
            "medicina", "pronto socorro", "pronto atendimento",
            "posto de saúde", "posto de saude"
        ]
        self.state_filters = [
            "rj", "rio de janeiro", "macaé", "macae", 
            "região dos lagos", "regiao dos lagos", "rio das ostras", 
            "campos", "campos dos goytacazes", "carapebus", 
            "quissamã", "quissama", "cabo frio", "búzios", "buzios",
            "são joão da barra", "sao joao da barra", "casimiro",
            "saquarema", "araruama", "arraial do cabo"
        ]

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
        for keyword in self.keywords:
            pattern = r'\b' + re.escape(keyword) + r'\b'
            
            if re.search(pattern, text_lower):
                return True
        return False

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
    

class PCISaudeScraper(BaseScraper):
    """
    Substituto do RSS. Scraper focado na página de Saúde geral do PCI.
    """
    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/saude/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")
            
            if not title or "concursos" not in link_href:
                continue

            parent_block = link_element.parent
            if not parent_block:
                continue
                
            block_text = parent_block.text.strip()

            # Na aba de saúde, focamos fortemente no filtro regional e de palavras
            if self.is_in_target_state(block_text) and self.is_relevant(block_text):
                
                pub_date = datetime.now().strftime("%Y-%m-%d")
                
                found_jobs.append({
                    "title": f"[PCI Saúde] {title}",
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": pub_date
                })

        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[PCISaude] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class PCIEstadualScraper(BaseScraper):
    """Focado exclusivamente na listagem completa do Estado do Rio de Janeiro."""
    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/rj/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")
            
            if not title or "concursos" not in link_href:
                continue

            parent_block = link_element.parent
            if not parent_block:
                continue
                
            block_text = parent_block.text.strip()

            # Passa no nosso filtro (com regex corrigido)?
            if self.is_in_target_state(block_text) and self.is_relevant(block_text):
                pub_date = datetime.now().strftime("%Y-%m-%d")
                
                found_jobs.append({
                    "title": f"[PCI RJ] {title}",
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": pub_date
                })

        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[PCIEstadual] Found {len(unique_jobs)} relevant medical jobs.")
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
    
class JCConcursosScraper(BaseScraper):
    """Scraper para o portal JC Concursos, buscando na página geral do RJ."""
    def __init__(self):
        super().__init__()
        # URL atualizada: Página principal de concursos do RJ
        self.url = "https://jcconcursos.com.br/concursos/rj"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        cards = soup.find_all("a", href=True)

        for card in cards:
            title = card.text.strip()
            link_href = card.get("href")
            
            if "/concursos/" not in link_href and "/noticia/" not in link_href:
                continue
                
            if len(title) < 15: # Evita capturar menus curtos
                continue

            # Agora nós usamos o filtro do bot para encontrar as vagas médicas dentro da página do RJ
            if self.is_relevant(title):
                
                full_link = link_href if link_href.startswith("http") else f"https://jcconcursos.com.br{link_href}"
                
                found_jobs.append({
                    "title": f"[JC Concursos] {title}",
                    "link": full_link,
                    "pub_date": datetime.now().strftime("%Y-%m-%d")
                })

        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[JCConcursos] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)
    

class BingNewsScraper(BaseScraper):
    """Busca em blogs médicos, portais de prefeituras e jornais locais através do Bing News."""
    def __init__(self):
        super().__init__()
        # Query já foca nas cidades-alvo e na carreira
        self.url = "https://www.bing.com/news/search?q=concurso+medico+rio+de+janeiro+OR+macae+OR+campos&qft=interval%3d%227%22" # Últimos 7 dias

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []
        
        # O Bing agrupa as notícias em div.news-card
        cards = soup.find_all("div", class_="news-card")

        for card in cards:
            link_element = card.find("a", class_="title")
            if not link_element:
                continue

            title = link_element.text.strip()
            link_href = link_element.get("href", "")
            
            # Aqui, analisamos tanto o título quanto o snippet da notícia
            snippet_element = card.find("div", class_="snippet")
            snippet = snippet_element.text.strip() if snippet_element else ""
            
            full_text = f"{title} {snippet}"

            if self.is_relevant(full_text):
                found_jobs.append({
                    "title": f"[Radar/News] {title}",
                    "link": link_href,
                    "pub_date": datetime.now().strftime("%Y-%m-%d")
                })

        unique_jobs = {job['link']: job for job in found_jobs}.values()
        logging.info(f"[BingNews] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)
    

# ==========================================
# PHASE 4: MAIN EXECUTION LOGIC
# ==========================================
if __name__ == "__main__":
    import os
    
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    # Pega a string do GitHub e transforma em uma lista do Python, removendo espaços
    raw_chat_ids = os.getenv("TELEGRAM_CHAT_IDS", "")
    TELEGRAM_CHAT_IDS = [chat_id.strip() for chat_id in raw_chat_ids.split(",") if chat_id.strip()]

    logging.info(f"Starting MedAlert RJ Scraper Engine... (Broadcasting to {len(TELEGRAM_CHAT_IDS)} chats)")

    db = DatabaseManager()
    notifier = TelegramNotifier(bot_token=TELEGRAM_BOT_TOKEN, chat_ids=TELEGRAM_CHAT_IDS)
    
    scrapers: List[BaseScraper] = [
        PCIScraper(),
        GoogleNewsScraper(),
        G1Scraper(),
        PCISaudeScraper(),
        JCConcursosScraper(),
        PCIEstadualScraper(),
        BingNewsScraper()
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
                
                msg = (
                    f"🚨 *Novo Processo Seletivo Encontrado!*\n\n"
                    f"🏥 *Vaga:* {job['title']}\n"
                    f"📅 *Data limite/Info:* {job['pub_date']}\n\n"
                    f"🔗 [Clique aqui para acessar o edital]({job['link']})"
                )
                
                # Envia para todos. Se pelo menos 1 pessoa receber, marcamos como enviado.
                sends = notifier.send_message(msg)
                if sends > 0:
                    db.mark_as_sent(job['link'])
                    messages_sent += sends

    logging.info(f"Execution finished. {new_jobs_count} new jobs added. {messages_sent} Telegram alerts sent.")
    
    # --- GERAR RELATÓRIOS APÓS A VARREDURA ---
    reporter = ReportGenerator(db_manager=db)
    reporter.generate_csv()
    reporter.generate_markdown()
    # -----------------------------------------------

    db.close()
    
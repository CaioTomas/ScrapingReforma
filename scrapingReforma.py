import re
import feedparser
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm  # Progress bar library

class GoogleNewsFeedScraper:
    def __init__(self, query, start_date, end_date):
        self.query = query
        self.start_date = start_date
        self.end_date = end_date

    def scrape_google_news_feed(self, start, end):
        # Formatar a consulta para o RSS do Google News com intervalo de datas
        formatted_query = '%20'.join(self.query.split())
        rss_url = (
            f'https://news.google.com/rss/search?q={formatted_query}'
            f'+after:{start}+before:{end}&hl=pt-BR&gl=BR&ceid=BR%3Apt'
        )
        feed = feedparser.parse(rss_url)

        # Listas para armazenar os dados
        titles = []
        links = []
        pubdates = []
        leads = []

        if feed.entries:
            for entry in feed.entries:
                # Capturar o título
                title = entry.title
                titles.append(title)
                
                # Capturar o link
                link = entry.link
                links.append(link)
                
                # Capturar a data de publicação
                pubdate = entry.published
                try:
                    date_obj = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %Z")
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    formatted_date = pubdate  # Usar o formato original caso não consiga converter
                pubdates.append(formatted_date)
                
                # Capturar o lead (resumo da notícia)
                lead = entry.summary if 'summary' in entry else "Resumo não disponível"
                leads.append(lead)

        # Montar o dicionário de dados
        data = {'Título': titles, 'Resumo': leads, 'Link': links, 'Data': pubdates}
        return data

    def get_news_in_batches(self):
        # Gerar intervalos mensais entre start_date e end_date
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")
        all_data = []

        date_ranges = []
        while start < end:
            batch_end = min(start + timedelta(days=30), end)
            date_ranges.append((start.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
            start = batch_end

        # Processar intervalos com barra de progresso
        for start_date, end_date in tqdm(date_ranges, desc="Fetching news batches"):
            batch_data = self.scrape_google_news_feed(start_date, end_date)
            
            batch_data['Veículo'] = [ titulo.split("-")[-1].strip() for titulo in batch_data['Título'] ]
            batch_data['Resumo clean'] = [ re.search(r'<a [^>]*>(.*?)</a>', resumo).group(1) for resumo in batch_data['Resumo'] ]
            
            all_data.extend(
                zip(batch_data['Título'], batch_data['Veículo'], batch_data['Resumo clean'], batch_data['Resumo'], batch_data['Link'], batch_data['Data'])
            )

        return all_data

    def convert_data_to_csv(self):
        all_data = self.get_news_in_batches()
        df = pd.DataFrame(all_data, columns=['Título', 'Veículo', 'Resumo', 'Resumo bruto', 'Link', 'Data'])
        # Nome do arquivo CSV baseado na consulta
        csv_name = f"{self.query}_{self.start_date}_to_{self.end_date}.csv"
        csv_name_new = csv_name.replace(" ", "_")
        df.to_csv(csv_name_new, index=False, encoding='utf-8-sig')
        print(f"Dados salvos no arquivo: {csv_name_new}")


if __name__ == "__main__":
    query = 'reforma tributária'
    start_date = '2019-01-01'  # Data inicial no formato YYYY-MM-DD
    end_date = '2023-12-20'    # Data final no formato YYYY-MM-DD

    scraper = GoogleNewsFeedScraper(query, start_date, end_date)
    scraper.convert_data_to_csv()

import re
import wikipediaapi
import pandas as pd
from tqdm import tqdm
import time
from pathlib import Path

class UzbekWikiScraper:
    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia(
            language='uz',
            extract_format=wikipediaapi.ExtractFormat.WIKI,
            user_agent='UzbekWikiScraper/1.0'
        )
        self.data_dir = Path('data/raw')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cleaner = TextCleaner()
        
    def scrape_page(self, title):
        page = self.wiki.page(title)
        if page.exists():
            return {
                'title': page.title,
                'text': page.text,
                'url': page.fullurl,
                'length': len(page.text)
            }
        return None

    def save_batch(self, data, batch_num):
        save_dir = Path('data/scraped/latin')
        save_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data)
        df.to_csv(save_dir / f'batch_{batch_num}.csv', index=False)

    def get_category_members(self, category_name):
        category = self.wiki.page(f"Category:{category_name}")
        members = []

        if category.exists():
            for member in category.categorymembers.values():
                members.append(member.title)
                
        return members

    def scrape_category(self, category_name, batch_size=100):
        members = self.get_category_members(category_name)
        batches = [members[i:i + batch_size] for i in range(0, len(members), batch_size)]
        
        for i, batch in enumerate(batches):
            data = []
            for title in tqdm(batch):
                result = self.scrape_page(title)
                if result:
                    data.append(result)
                time.sleep(1)  # Rate limiting
            
            if data:
                self.save_batch(data, i)

    def scrape_from_urls(self, urls_file):
        df = pd.read_csv(urls_file)
        data = []
        
        for _, row in tqdm(df.iterrows()):
            url = row['url']
            title = url.split('/')[-1].replace('_', ' ')
            result = self.scrape_page(title)
            if result:
                data.append(result)
            time.sleep(1)
        
        self.save_batch(data, 'from_urls')

    def get_subcategories(self, category_name):
        category = self.wiki.page(f"Category:{category_name}")
        subcats = []
        
        if category.exists():
            for member in category.categorymembers.values():
                if 'Category:' in member.title:
                    subcats.append(member.title.replace('Category:', ''))
        return subcats

    def recursive_scrape(self, category_name, max_depth=2, max_articles=1000, visited=None, article_count=0):
        if visited is None:
            visited = set()
            
        if max_depth < 0 or category_name in visited or article_count >= max_articles:
            return article_count
            
        print(f"Category: {category_name} | Articles: {article_count}/{max_articles}")
        visited.add(category_name)
        
        members = self.get_category_members(category_name)
        article_count += len(members)
        self.scrape_category(category_name)
        
        subcats = self.get_subcategories(category_name)
        for subcat in subcats:
            article_count = self.recursive_scrape(subcat, max_depth-1, max_articles, visited, article_count)
            
        return article_count

    def get_all_articles(self):
        import requests
        
        batch_size = 10000
        current_batch = []
        batch_number = 0
        total_collected = 0
        
        stats_params = {
            "action": "query",
            "meta": "siteinfo", 
            "siprop": "statistics",
            "format": "json"
        }
        response = requests.get("https://uz.wikipedia.org/w/api.php", params=stats_params)
        total_articles = response.json()['query']['statistics']['articles']
        
        continue_token = None
        
        while True:
            params = {
                "action": "query",
                "list": "allpages",
                "aplimit": "500",
                "apnamespace": "0",
                "format": "json"
            }
            if continue_token:
                params['apcontinue'] = continue_token
                
            response = requests.get("https://uz.wikipedia.org/w/api.php", params=params)
            data = response.json()
            
            if 'query' in data and 'allpages' in data['query']:
                for page in data['query']['allpages']:
                    current_batch.append(page['title'])
                    total_collected += 1
                    if len(current_batch) >= batch_size:
                        pd.DataFrame({'title': current_batch}).to_csv(
                            self.data_dir / f'titles_batch_{batch_number}.csv', 
                            index=False
                        )
                        current_batch = []
                        batch_number += 1
                        
            progress = (total_collected / total_articles) * 100
            print(f"Progress: {progress:.1f}% ({total_collected}/{total_articles} articles)")
            
            if 'continue' not in data:
                break
                    
            continue_token = data['continue']['apcontinue']
            time.sleep(1)
        
        # Save any remaining titles
        if current_batch:
            pd.DataFrame({'title': current_batch}).to_csv(
                self.data_dir / f'titles_batch_{batch_number}.csv', 
                index=False
            )

    def scrape_all_articles(self, batch_size=100):
        articles = self.get_all_articles()
        print(f"Total articles to scrape: {len(articles)}")
        
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]
        
        for i, batch in enumerate(batches):
            print(f"Processing batch {i+1}/{len(batches)}")
            self.scrape_category(batch, batch_size)

    def clean_and_save(self, batch_data):
        cleaned_data = []
        for item in batch_data:
            item['text'] = self.cleaner.clean_text(item['text'])
            if len(item['text']) > 100:
                cleaned_data.append(item)
        return cleaned_data

    def parallel_scrape_from_files(self, batch_size=100, max_workers=4):
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from glob import glob
        import os

        def scrape_batch(titles):
            results = []
            for title in titles:
                try:
                    result = self.scrape_page(title)
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error scraping {title}: {str(e)}")
                    continue
                time.sleep(1)
            return results

        title_files = glob(os.path.join(self.data_dir, 'titles/latin/titles_batch_*.csv'))
        
        for file_num, file in enumerate(title_files):
            print(f"Processing file {file_num + 1}/{len(title_files)}")
            df = pd.read_csv(file)
            titles = df['title'].tolist()
            batches = [titles[i:i + batch_size] for i in range(0, len(titles), batch_size)]
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {executor.submit(scrape_batch, batch): i for i, batch in enumerate(batches)}
                
                for future in as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    try:
                        results = future.result()
                        if results:
                            cleaned_results = self.clean_and_save(results)
                            if cleaned_results:
                                self.save_batch(cleaned_results, f"{file_num}_{batch_num}")
                    except Exception as e:
                        print(f"Batch {batch_num} failed: {str(e)}")

    def scrape_from_title_files(self, title_files_dir, output_dir, batch_size=50):
        """
        Scrape Wikipedia pages from title CSV files.
        
        Args:
            title_files_dir (str): Directory containing the title CSV files
            output_dir (str): Directory to save the scraped data
            batch_size (int): Number of pages to process in each batch
        """
        from pathlib import Path
        import pandas as pd
        from tqdm import tqdm
        import time
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Get all CSV files in the title files directory
        title_files = list(Path(title_files_dir).glob('*.csv'))
        
        for file_num, title_file in enumerate(title_files):
            print(f"\nProcessing file {file_num + 1}/{len(title_files)}: {title_file.name}")
            
            # Read the title CSV file
            df = pd.read_csv(title_file)
            titles = df['title'].tolist()
            
            # Process titles in batches
            for batch_num, i in enumerate(range(0, len(titles), batch_size)):
                batch_titles = titles[i:i + batch_size]
                batch_data = []
                
                print(f"\nProcessing batch {batch_num + 1}/{len(titles)//batch_size + 1}")
                
                # Scrape each title in the batch
                for title in tqdm(batch_titles):
                    try:
                        result = self.scrape_page(title)
                        if result and len(result['text']) > 100:  # Filter out very short articles
                            # Clean the text
                            result['text'] = self.cleaner.clean_text(result['text'])
                            batch_data.append(result)
                    except Exception as e:
                        print(f"Error scraping {title}: {str(e)}")
                        continue
                    
                    time.sleep(1)  # Rate limiting
                
                # Save the batch if we have data
                if batch_data:
                    batch_df = pd.DataFrame(batch_data)
                    output_file = output_path / f'wiki_content_{file_num}_{batch_num}.csv'
                    batch_df.to_csv(output_file, index=False)
                    print(f"Saved batch to {output_file}")
                    
                print(f"Processed {len(batch_data)} articles in this batch")

class TextCleaner:
   def __init__(self):
       self.unwanted_sections = ['Havolalar', 'Manbalar', 'Izohlar']
       
   def clean_text(self, text):
       # Remove unwanted sections
       for section in self.unwanted_sections:
           if section in text:
               text = text.split(section)[0]
       
       # Remove special characters and extra whitespace
       text = re.sub(r'\[\d+\]', '', text)  # Remove citation brackets [1], [2] etc
       text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
       text = text.strip()
       
       return text

if __name__ == '__main__':
    scraper = UzbekWikiScraper()

scraper = UzbekWikiScraper()
# scraper.scrape_all_articles()
# scraper.parallel_scrape_from_files(batch_size=50, max_workers=4)
scraper.scrape_from_title_files(
    title_files_dir='data/titles/latin',
    output_dir='data/content/latin',
    batch_size=50
)
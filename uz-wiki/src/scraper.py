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

    def check_processed_batches(self, title_files_dir, output_dir):
        """
        Check which batches have been processed by examining output files.
        
        Args:
            title_files_dir (str): Directory containing the title CSV files
            output_dir (str): Directory containing the output data
        """
        from pathlib import Path
        import pandas as pd
        import re
        
        output_path = Path(output_dir)
        processed_files = set()
        
        print("Checking existing output files...")
        existing_files = sorted(list(output_path.glob('wiki_content_1_*.csv')))
        print(f"Found {len(existing_files)} output files")
        
        for file in existing_files:
            print(f"\nChecking {file.name}")
            try:
                df = pd.read_csv(file)
                if not df.empty:
                    # Get the first title
                    title = df['title'].iloc[0]
                    print(f"Found title: {title}")
                    
                    # Find which input file contains this title
                    for batch_file in Path(title_files_dir).glob('titles_batch_*.csv'):
                        batch_df = pd.read_csv(batch_file)
                        if title in batch_df['title'].values:
                            match = re.search(r'titles_batch_(\d+)\.csv', batch_file.name)
                            if match:
                                batch_num = int(match.group(1))
                                processed_files.add(batch_num)
                                print(f"This comes from batch {batch_num}")
                                break
            except Exception as e:
                print(f"Error reading {file}: {e}")

        print("\nSummary:")
        print(f"Total processed batches: {len(processed_files)}")
        if processed_files:
            print(f"Processed batch numbers: {sorted(list(processed_files))}")
            print(f"Last processed batch: {max(processed_files)}")

    def scrape_from_title_files(self, title_files_dir, output_dir, output_prefix="wiki_content", batch_size=50):
        """
        Scrape Wikipedia pages from title CSV files with resume functionality.
        
        Args:
            title_files_dir (str): Directory containing the title CSV files
            output_dir (str): Directory to save the scraped data
            output_prefix (str): Prefix for output files (default "wiki_content")
            batch_size (int): Number of pages to process in each batch
        """
        from pathlib import Path
        import pandas as pd
        from tqdm import tqdm
        import time
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Find the last processed title from any output file
        last_title = None
        existing_files = sorted(list(output_path.glob('*.csv')))
        if existing_files:
            try:
                last_file = existing_files[-1]
                df = pd.read_csv(last_file)
                if not df.empty:
                    last_title = df['title'].iloc[0]
                    print(f"Last processed title: {last_title}")
            except Exception as e:
                print(f"Error reading last file: {e}")
        
        # Get all title CSV files
        title_files = sorted(list(Path(title_files_dir).glob('*.csv')))
        print(f"Found {len(title_files)} title files")
        
        # Process titles
        found_last_title = last_title is None  # If no last title, start from beginning
        for title_file in title_files:
            print(f"\nReading file: {title_file.name}")
            
            # Read titles from file
            df = pd.read_csv(title_file)
            titles = df['title'].tolist()
            
            # Sort titles alphabetically to ensure consistent ordering
            titles.sort()
            
            # Split into batches
            batches = [titles[i:i + batch_size] for i in range(0, len(titles), batch_size)]
            
            for batch_num, batch_titles in enumerate(batches):
                # Skip until we find the last processed title
                if not found_last_title:
                    if last_title in batch_titles:
                        found_last_title = True
                    continue
                
                print(f"\nProcessing batch {batch_num + 1}/{len(batches)}")
                batch_data = []
                
                # Process each title in the batch
                for title in tqdm(batch_titles):
                    try:
                        result = self.scrape_page(title)
                        if result and len(result['text']) > 100:
                            result['text'] = self.cleaner.clean_text(result['text'])
                            batch_data.append(result)
                    except Exception as e:
                        print(f"Error scraping {title}: {e}")
                        continue
                    time.sleep(1)  # Rate limiting
                
                # Save the batch if we have data
                if batch_data:
                    next_file_num = len(existing_files) + 1
                    output_file = output_path / f'{output_prefix}_{next_file_num}.csv'
                    batch_df = pd.DataFrame(batch_data)
                    batch_df.to_csv(output_file, index=False)
                    existing_files.append(output_file)
                    print(f"Saved {len(batch_data)} articles to {output_file}")

    def parallel_scrape_from_files(self, title_files_dir, output_dir, output_prefix="wiki_content", batch_size=50, max_workers=4):
        """
        Parallel scrape Wikipedia pages with resume functionality.
        
        Args:
            title_files_dir (str): Directory containing the title CSV files
            output_dir (str): Directory to save the scraped data
            output_prefix (str): Prefix for output files
            batch_size (int): Number of pages to process in each batch
            max_workers (int): Number of parallel workers
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from pathlib import Path
        import pandas as pd
        from tqdm import tqdm
        import time
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        def scrape_batch(titles, batch_id):
            results = []
            for title in tqdm(titles, desc=f"Batch {batch_id}", leave=False):
                try:
                    result = self.scrape_page(title)
                    if result and len(result['text']) > 100:
                        result['text'] = self.cleaner.clean_text(result['text'])
                        results.append(result)
                except Exception as e:
                    print(f"Error scraping {title}: {e}")
                    continue
                time.sleep(1)
            return results

        # Find the last processed title
        last_title = None
        existing_files = sorted(list(output_path.glob('*.csv')))
        if existing_files:
            try:
                last_file = existing_files[-1]
                df = pd.read_csv(last_file)
                if not df.empty:
                    last_title = df['title'].iloc[0]
                    print(f"Last processed title: {last_title}")
            except Exception as e:
                print(f"Error reading last file: {e}")

        # Get all titles and sort them alphabetically
        all_titles = []
        title_files = sorted(list(Path(title_files_dir).glob('*.csv')))
        for file in title_files:
            df = pd.read_csv(file)
            # Convert all titles to strings
            titles = [str(title) for title in df['title'].tolist()]
            all_titles.extend(titles)

        all_titles.sort()
        print(f"Total titles: {len(all_titles)}")
        
        # Find where to start
        if last_title:
            try:
                start_idx = all_titles.index(last_title) + 1
                all_titles = all_titles[start_idx:]
                print(f"Continuing from index {start_idx}")
            except ValueError:
                print(f"Couldn't find last title {last_title}, starting from beginning")
        
        # Split remaining titles into batches
        batches = [all_titles[i:i + batch_size] for i in range(0, len(all_titles), batch_size)]
        print(f"Split into {len(batches)} batches of size {batch_size}")
        
        # Process batches in parallel
        next_file_num = len(existing_files) + 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(scrape_batch, batch, i): i 
                for i, batch in enumerate(batches)
            }
            
            for future in as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    results = future.result()
                    if results:
                        batch_df = pd.DataFrame(results)
                        output_file = output_path / f'{output_prefix}_{next_file_num}.csv'
                        batch_df.to_csv(output_file, index=False)
                        print(f"\nSaved batch {batch_num} to {output_file} ({len(results)} articles)")
                        next_file_num += 1
                except Exception as e:
                    print(f"\nBatch {batch_num} failed: {e}")

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
# scraper.scrape_from_title_files(
#     title_files_dir='data/titles/latin',
#     output_dir='data/content/latin',
#     batch_size=50
# )
scraper.parallel_scrape_from_files(
    title_files_dir='data/titles/latin',
    output_dir='data/content/latin',
    output_prefix='wiki_content',
    batch_size=50,
    max_workers=4
)
# scraper.check_processed_batches(
#     title_files_dir='data/titles/latin',
#     output_dir='data/content/latin'
# )
from datetime import datetime
from getpass import getpass
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from pandas import DataFrame

# Keywords to scrape
keywords=["seneau","sen'eau","Sen'Eau","Sen'eau","Seneau","Forum Mondial de l'eau",
        "Eau du Senegal", "SEN'EAU","SONES","Hydraulique Senegal", "Contrat d'affermage",
        "distribution eau", "qualite eau", "facture eau", "Forum civil eau", "POSCEAS",
        "distribution-eau", "eau-senegal", "facture-eau", "qualité-eau","hydraulique"]

##############################################################################
#                SCRAPING ALL ARTICLES IN THE PAGES                          #
##############################################################################

# Initiliasing Dataframe to save scraped data
article_df = DataFrame(columns=['SiteName', 'PageLink', 'CapturedKeyword', 'ArticleTitle', 
                'ArticleCategory', 'NumberViews', 'CommentsNumber', 'PostDate', 'CreatedAt'])

# Grepping data and save as a pandas Dataframe
for i in range(len(keywords)) :

    url = f"https://www.seneweb.com/news/{keywords[i]}/"
    page = requests.get(url).text
    doc = BeautifulSoup(page, 'html.parser')

    articles = doc.find_all('li', class_ = 'module_news_item')

    for article in articles:
        try:
            # Grepping key data
            page_link = article.find('a', class_ = "module_news_item_title")['href']
            article_category = article.find('div', class_ = "module_news_item_content").find('span', class_ = "module_news_item_categ").text
            article_title = article.find('div', class_ = "module_news_item_content").a.text
            views_number = int(article.find('span', class_ = "meta_item meta_views").text)
            comments_number = int(article.find('span', class_ = "meta_item meta_comments").text)
            post_date = article.find('span', class_ = "meta_item meta_date").text

            d_timestamp = datetime.now()
        
            # Reformate to a dict
            dict_obj = {
                'SiteName': "SENEWEB",
                'PageLink': page_link, 
                'CapturedKeyword': keywords[i], 
                'ArticleTitle': article_title, 
                'ArticleCategory': article_category, 
                'NumberViews': views_number,
                'CommentsNumber': comments_number,
                'PostDate' : post_date,
                'CreatedAt': d_timestamp
            }

            # Saving as a pandas Dataframe (appending)
            article_df = article_df.append(dict_obj, ignore_index=True)

        except:
            pass


##############################################################################
#                INDEXING SCRAPED DATA TO ELASTICSEARCH                      #
##############################################################################
# ES Client
es_host = "https://m11896.contaboserver.net:9200"
user = "admin"
pwd = "DataBeez@221"

es = Elasticsearch(
    hosts=es_host,
    http_auth=(user, pwd),
    verify_certs=False
    )

# Indexing (adding) documents
docs = article_df.iterrows()

for index, doc in docs :
    docdict = {
        'SiteName': f"{doc['SiteName']}",
        'PageLink' : f"{doc['PageLink']}",
        'CapturedKeyword': f"{doc['CapturedKeyword']}",
        'ArticleTitle': f"{doc['ArticleTitle']}",
        'ArticleCategory': f"{doc['ArticleCategory']}",
        'NumberViews': doc['NumberViews'],
        'CommentsNumber': doc['CommentsNumber'],
        'PostDate': f"{doc['PostDate']}",
        'CreatedAt': doc['CreatedAt'],
        }
    res = es.index(index="sen_scraped_data", body=docdict) #ES 7.10, pour ES 8.X, on a document=docdict


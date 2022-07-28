import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def get_data(keyword):
    url="https://www.seneweb.com"+keyword
    response = requests.get(url)
    page_contents = response.text
    doc = BeautifulSoup(page_contents, 'html.parser')
    return doc


urls_distinct=[]

data_sones=get_data("/news/SONES")
print(data_sones.prettify())


def getArticles(soup):
    articles = soup.find_all(attrs={"class":["module_news_item",'category_post_content']})
    questionList = []
    for item in articles:
        if all(item.find('a', {'class':['module_news_item_title','category_post_title']})['href']!=j for j in urls_distinct):
            article = {
            'link' : item.find('a', {'class':['module_news_item_title','category_post_title']})['href'],
            'date_pub' : item.find('span', {'class':'meta_item meta_date'}).text,
            'nb_views' : item.find('span', {'class':'meta_item meta_views'}).text,
            'nb_comment' : item.find('span', {'class':'meta_item meta_comments'}).text
            }
            urls_distinct.append(item.find('a', {'class':['module_news_item_title','category_post_title']})['href'])
            questionList.append(article)
    return questionList

articles=getArticles(data_sones)


for i in articles:
  print(i)


article_df = pd.DataFrame(columns=['SiteName', 'PageLink', 'CapturedKeyword', 'ArticleTitle', 'ArticleCategory', 'Comments', 'PostDate','NumberViews','like','dislike'])

def preprocess(s):
    # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"[^\w\s]", ' ', s)
    # Replace all runs of whitespaces with no space
    s = re.sub(r"\s+", ' ', s)
    # replace digits with no space
    s = re.sub(r"\d", ' ', s)
  
    return s


for link_ in articles:
    # Retrieving some informations
    docum = get_data(link_["link"])
    main_site = "SENEWEB" # Nom du scraped website
    article_title = docum.h1 # Titre de l'article
    post_category = docum.find(class_="post_header_categ") # Categorie du poste
    captured_kw = "SONES" # Title de la catégorie / Mot clé capturé

    body_tag = docum.find_all(class_='comment_item_content')
    comments=[]
    for i in body_tag:
      comments.append({
          "like":i.find(class_='btn_tnumb_up').text,
          "dislike":i.find(class_='btn_tnumb_down').text,
          "comment":i.find('span',{'style':'display:block;padding:10px 10px 10px 10px;color:#000000'}).text
      })
    
    for comment in comments:
        commentaire=preprocess(comment['comment'])# Liste des commentaires de l'article
        like=comment['like'].replace("+","").replace("-","")
        dislike=comment['dislike'].replace("+","").replace("-","")
        # Saving grepped data as pandas Dataframe
        article_df = article_df.append({'SiteName':main_site,'PageLink':link_["link"],
                    'CapturedKeyword':captured_kw, 'ArticleTitle':article_title,
                    'ArticleCategory':post_category, 'Comments':commentaire, 'PostDate': link_["date_pub"],
                    'NumberViews':link_["nb_views"],'like':like,'dislike':dislike},
                    ignore_index=True)

 



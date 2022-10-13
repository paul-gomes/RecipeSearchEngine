import pandas as pd
from bs4 import BeautifulSoup 
import requests
import pickle
import json

column_names = ["a", "b"]
recipe_url_df = pd.DataFrame(columns = column_names)

#URLs collection from website 1 (Jamieoliver.com)
j_recipe_df = pd.DataFrame()
j_recipe_list = []
start_urls = ["https://www.jamieoliver.com/recipes/category/course/mains/"]

s_url_num = 1
for u in start_urls:
    if s_url_num > 100 or len(j_recipe_list) >= 2000:
        break
    print(f"processing w1 -- {s_url_num} -- {u}")
    page = requests.get(u)
    soup = BeautifulSoup(page.text, "html.parser")
    recipe_urls = pd.Series([a.get("href") for a in soup.find_all("a")])
    recipe_urls = recipe_urls[(recipe_urls.str.contains("/recipes/")==True)
                          & (recipe_urls.str.endswith("/recipes/")==False)
                          & (recipe_urls.str.contains("books")==False)
                          ].unique()
    recipe_urls = ["https://www.jamieoliver.com" + u for u in recipe_urls if "www.jamieoliver.com" not in u]
    #insert url into start_urls for processing if not the recipe page
    for url in recipe_urls:
        try:
            soup_new = BeautifulSoup(requests.get(url).content, 'html.parser')
            if(soup_new.find('script', type="application/ld+json")):
                res = json.loads(soup_new.find('script', type="application/ld+json").string)
                if 'name' not in res:
                    start_urls.append(url)
            else:
                start_urls.append(url)
        except:
            continue
            
    j_recipe_list.extend([u for u in recipe_urls if start_urls.__contains__(u) == False]) 
    s_url_num += 1
    print(f"Recipe urls retrieved-- {len(j_recipe_list)}")

j_recipe_set = set(j_recipe_list)
j_recipe_df = j_recipe_df.append(pd.DataFrame(j_recipe_set, columns=['a']), ignore_index=True)


#URLs collection from website 2 (www.allrecipes.com)
a_recipe_df = pd.DataFrame()   
a_recipe_urls = []        
start_urls = ["https://www.allrecipes.com/recipes/78/breakfast-and-brunch/"]

s_url_num = 1
for u in start_urls:
    if s_url_num > 100 or len(a_recipe_urls) >= 2000:
        break
    print(f"processing w2 -- {s_url_num} -- {u}")
    page = requests.get(u)
    soup = BeautifulSoup(page.text, "html.parser")
    main_container = soup.select_one(".loc .fixedContent")
    if main_container is not None:
        recipe_urls = pd.Series([a.get("href") for a in main_container.find_all("a")])
        recipe_urls = [u for u in recipe_urls if ("www.allrecipes.com" in u)
                        & (start_urls.__contains__(u) == False)
                        & ("recipes/77/drinks/" not in u)
                        & ("/gallery/" not in u)
                        & ("/article/" not in u)]
        #insert url into start_urls for processing if not the recipe page
        for url in recipe_urls:
            try:
                soup_new = BeautifulSoup(requests.get(url).content, 'html.parser')
                if(soup_new.find('script', type="application/ld+json")):
                    response = json.loads(soup_new.find('script', type="application/ld+json").string)
                    res = response[0]
                    if 'name' not in res:
                        start_urls.append(url)
                else:
                    start_urls.append(url)
            except:
                continue
                
        a_recipe_urls.extend([u for u in recipe_urls if start_urls.__contains__(u) == False])   
        s_url_num += 1
        print(f"Recipe urls retrieved-- {len(a_recipe_urls)}")

a_recipe_set = set(a_recipe_urls)
a_recipe_df = a_recipe_df.append(pd.DataFrame(a_recipe_set, columns=['b']), ignore_index=True)
    
    
recipe_url_df = pd.concat([j_recipe_df, a_recipe_df], axis=1)

#save the url queue as pickle file
with open('../data/recipe_urls.pickle', 'wb') as handle:
    pickle.dump(recipe_url_df, handle, protocol=pickle.HIGHEST_PROTOCOL)
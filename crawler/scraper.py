import pandas as pd
from bs4 import BeautifulSoup 
import requests
import pickle
import json



with open('../data/recipe_urls.pickle', 'rb') as handle:
    recipe_urls_df = pickle.load(handle)

    

column_names = ["Title", "Ingredients", "Instructions", "CookingTime", "Keywords", "NutritionValue", "Source"]
recipes = pd.DataFrame(columns = column_names)

for i in range(len(recipe_urls_df)):
    #process website 1
    w1 = recipe_urls_df['a'][i]
    if not pd.isnull(w1):
        soup = BeautifulSoup(requests.get(w1).content, 'html.parser')
        res = json.loads(soup.find('script', type="application/ld+json").string)
        if 'name' in res:
            title = res['name']
        else:
            title = None
        
        if 'recipeIngredient' in res:
            ingredients = res['recipeIngredient']
        else:
            ingredients = None
        
        if 'recipeInstructions' in res:
            instructions = BeautifulSoup(res['recipeInstructions'], "html").text
        else:
            instructions = None
        
        if 'totalTime' in res:
            cook_time = res['totalTime']
        else:
            cook_time = None
            
        if 'keywords' in res:
            keywords = res['keywords']
        else:
            keywords = None
            
        if 'nutrition' in res:
            nutrition = res['nutrition']
        else:
            nutrition = None
        #soupjs.append(res)
        recipes = recipes.append({'Title': title, 'Ingredients': ingredients, 'Instructions': instructions,'CookingTime': 
          cook_time, 'Keywords': keywords, 'NutritionValue': nutrition, 'Source': w1}, ignore_index=True)
        print(f'{i} is now done [website 1]')
    
    
    #process website 2
    w2 = recipe_urls_df['b'][i]
    if not pd.isnull(w2):
        soup = BeautifulSoup(requests.get(w2).content, 'html.parser')
        response = json.loads(soup.find('script', type="application/ld+json").string)
        res = response[0]
        if 'name' in res:
            title = res['name']
        else:
            title = None
            
        if 'recipeIngredient' in res:
            ingredients = res['recipeIngredient']
        else:
            ingredients = None
            
        if 'recipeInstructions' in res:
            instructions = ' '.join([i['text'] for i in res['recipeInstructions']])
        else:
            instructions = None
            
        if 'totalTime' in res:
            cook_time = res['totalTime']
        else:
            cook_time = None
            
        if 'keywords' in res:
            keywords = res['keywords']
        else:
            keywords = None
            
        if 'nutrition' in res:
            n = res['nutrition']
            del n["@type"]
            nutrition = n
        else:
            nutrition = None
        #soupjs.append(res)
        recipes = recipes.append({'Title': title, 'Ingredients': ingredients, 'Instructions': instructions,'CookingTime': 
          cook_time, 'Keywords': keywords, 'NutritionValue': nutrition, 'Source': w2}, ignore_index=True)
        print(f'{i} is now done [website 2]')
        print('------------------------------')
    
    #Lets start with small number of recipes
    if(i > 1000):
        break
    

with open('../data/recipes.pickle', 'wb') as handle:
    pickle.dump(recipes, handle, protocol=pickle.HIGHEST_PROTOCOL)
import pandas as pd
from bs4 import BeautifulSoup 
import requests
import pickle
import json
import asyncio
import sys
sys.path.insert(0,"..")
from services.azure_cosmos import *

def cookTimeInMin(text):
    try:
        prev_char = ''
        curr_char = ''
        cook_time = 0
        num = ''
        for c in text:
            curr_char = c
            if curr_char.lower() == 'h':
                if prev_char.isnumeric():
                    cook_time +=  int(num) * 60
                    num = ''
            if curr_char.lower() == 'm':
                if prev_char.isnumeric():
                    cook_time += int(num)
                    num = ''
            if curr_char.lower() == 's':
                if prev_char.isnumeric():
                    cook_time += int(num)/60
                    num = ''
            if curr_char.isnumeric():
                num += curr_char
            prev_char = c
        return round(cook_time, 2)
    except:
        print(f"Exception occured when converting cook time -- {text}")
            




if __name__=="__main__":
    
    with open('../data/recipes.pickle', 'rb') as handle:
        recipes = pickle.load(handle)
    
    column_names = ["id", "title", "ingredients", "instructions", "cookingTime", "keywords", 
                    "nutritionValue", "source", "cookingTimeInMin", "ingredientsCount", "calorieCount" ]
    recipes_cleaned = pd.DataFrame(columns = column_names)

    database_name = 'RecipesDb'
    container_name = 'RecipesContainer'
    
    db = DbClient()
    db_obj = db.get_or_create_db(database_name)
    recipe_container = db.get_or_create_container(db_obj, container_name, 'recipe')

    for i in range(len(recipes)):        
        recipe_dict = dict(recipes.iloc[i,:])
        recipe_dict.update({"id": str(i+1)})
        if(recipe_dict['cookingTime']):
            recipe_dict.update({"cookingTimeInMin": cookTimeInMin(recipe_dict['cookingTime'])})
        else:
            recipe_dict.update({"cookingTimeInMin": ''})
        
        if(recipe_dict['ingredients']):
            recipe_dict.update({"ingredientsCount": len(recipe_dict['ingredients'])})
        else:
            recipe_dict.update({"ingredients": ''})
        
        if(recipe_dict['nutritionValue'] and recipe_dict['nutritionValue']['calories']):
            c = recipe_dict['nutritionValue']['calories']
            calorie_num = int([int(s) for s in c.split() if s.isdigit()][0])
            recipe_dict.update({"calorieCount": calorie_num })
        else:
            recipe_dict.update({"calorieCount": ''})
        
        recipes_cleaned = recipes_cleaned.append(recipe_dict, ignore_index=True)
        insert_data = db.save_record(recipe_container, recipe_dict)
        print(f'-- item {i} is processed --') 
                                   
with open('../data/recipes_cleaned.pickle', 'wb') as handle:
    pickle.dump(recipes_cleaned, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
        
        

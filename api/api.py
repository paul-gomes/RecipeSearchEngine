from flask import Flask
import numpy as np
import pickle
from nltk.tokenize import word_tokenize
from collections import Counter
import math
import pandas as pd
from flask import jsonify
import sys
sys.path.insert(0,"..")
from services.azure_cosmos import *
from services.azure_storage import *
from services.utility_func import *
from scipy.spatial.distance import cdist
import itertools

app = Flask(__name__)


@app.route('/search/<string:query>/', methods=['GET'])
def scored_relevent_docs(query):
    database_name = 'RecipesDb'
    r_container_name = 'RecipesContainer'
    storage_container_name = 'documentscore'
    
    #Database client
    db = DbClient()
    db_obj = db.get_or_create_db(database_name)
    recipe_container = db.get_or_create_container(db_obj, r_container_name, 'recipe')
    
    #Storage client
    storage = AzureStorageClient()
    storage_container = storage.get_or_create_container(storage_container_name)
    
    #get all recipes from the datbase
    dbquery = "select * from RecipesContainer"
    data = db.execute_query(recipe_container, dbquery)
    data = pd.DataFrame(data, index=[int(x['id']) for x in data])
    
    term_df = storage.download_file(storage_container, 'term_df')
    tf_idf = storage.download_file(storage_container, 'tf_idf')
    
    total_vocab = [x for x in term_df]
    total_docs = [term_df[x] for x in term_df]
    total_docs = set().union(*total_docs)
    N = len(total_docs)
    
    doc_vector = np.zeros((N, len(total_vocab)))
    for i in tf_idf:
        try:
            index = total_vocab.index(eval(i)[1])
            doc_vector[int(eval(i)[0])][index] = tf_idf[i]
        except:
            pass
        
    s_vector = doc_query_similarity(20, query, doc_vector, N, total_vocab, term_df)
    print(s_vector)
    result = []
    for i in s_vector:
        r = data.loc[i]
        output = {
            'DocId': r['id'],
            'Title': r['title'],
            'Ingredients': r['ingredients'],
            'Instructions': r['instructions'],
            'CookingTime': r['cookingTime'],
            'Keywords': r['keywords'],
            'NutritionValue': r['nutritionValue'],
            'Source': r['source'],
            'CookingTimeInMin': r['cookingTimeInMin'],
            'CalorieCount': r['calorieCount'],
            'IngredientsCount': r['ingredientsCount'] 
        }
        result.append(output)
    return jsonify(result)



@app.route('/searchWithTermProximity/<string:query>/', methods=['GET'])
def scored_relevent_docs_with_term_proximity(query):
    database_name = 'RecipesDb'
    r_container_name = 'RecipesContainer'
    storage_container_name = 'documentscore'
    
    #Database client
    db = DbClient()
    db_obj = db.get_or_create_db(database_name)
    recipe_container = db.get_or_create_container(db_obj, r_container_name, 'recipe')
    
    #Storage client
    storage = AzureStorageClient()
    storage_container = storage.get_or_create_container(storage_container_name)
    
    #get all recipes from the datbase
    dbquery = "select * from RecipesContainer"
    data = db.execute_query(recipe_container, dbquery)
    data = pd.DataFrame(data, index=[int(x['id']) for x in data])
    
    term_df = storage.download_file(storage_container, 'term_df')
    tf_idf = storage.download_file(storage_container, 'tf_idf')
    term_prox = storage.download_file(storage_container, 'term_prox')
    
    total_vocab = [x for x in term_df]
    total_docs = [term_df[x] for x in term_df]
    total_docs = set().union(*total_docs)
    N = len(total_docs)
    
    doc_vector = np.zeros((N, len(total_vocab)))
    for i in tf_idf:
        try:
            index = total_vocab.index(eval(i)[1])
            doc_vector[int(eval(i)[0])][index] = tf_idf[i]
        except:
            pass
        
    s_vector = doc_query_similarity_with_term_proximity(20, query, doc_vector, N, total_vocab, term_df, term_prox)
    print(s_vector)
    result = []
    for i in s_vector:
        r = data.loc[i]
        output = {
            'DocId': r['id'],
            'Title': r['title'],
            'Ingredients': r['ingredients'],
            'Instructions': r['instructions'],
            'CookingTime': r['cookingTime'],
            'Keywords': r['keywords'],
            'NutritionValue': r['nutritionValue'],
            'Source': r['source'],
            'CookingTimeInMin': r['cookingTimeInMin'],
            'CalorieCount': r['calorieCount'],
            'IngredientsCount': r['ingredientsCount'] 
        }
        result.append(output)
    return jsonify(result)



@app.route('/expand/<string:query>/<string:irrelevant_doc>/<string:relevant_docs>/', methods=['GET'])
def relevance_feedback(query, irrelevant_doc, relevant_docs):
    database_name = 'RecipesDb'
    r_container_name = 'RecipesContainer'
    storage_container_name = 'documentscore'
    
    #Database client
    db = DbClient()
    db_obj = db.get_or_create_db(database_name)
    recipe_container = db.get_or_create_container(db_obj, r_container_name, 'recipe')
    
    #Storage client
    storage = AzureStorageClient()
    storage_container = storage.get_or_create_container(storage_container_name)
    
    #get all recipes from the datbase
    dbquery = "select * from RecipesContainer"
    data = db.execute_query(recipe_container, dbquery)
    data = pd.DataFrame(data, index=[int(x['id']) for x in data])
    
    term_df = storage.download_file(storage_container, 'term_df')
    tf_idf = storage.download_file(storage_container, 'tf_idf')
    term_prox = storage.download_file(storage_container, 'term_prox')

    total_vocab = [x for x in term_df]
    total_docs = [term_df[x] for x in term_df]
    total_docs = set().union(*total_docs)
    N = len(total_docs)
    

    doc_vector = np.zeros((N, len(total_vocab)))
    for i in tf_idf:
        try:
            index = total_vocab.index(i[1])
            doc_vector[i[0]][index] = tf_idf[i]
        except:
            pass
    
    relevant_docs_v = [doc_vector[int(i)] for i in relevant_docs]
    irrelevant_doc_v = doc_vector[int(irrelevant_doc)]
    s_vector = doc_query_similarity_with_query_expansion(20, query, doc_vector, N, total_vocab, term_df, relevant_docs_v, irrelevant_doc_v)
    print(s_vector)
    result = []
    for i in s_vector:
        r = data.loc[i]
        output = {
            'DocId': r['id'],
            'Title': r['title'],
            'Ingredients': r['ingredients'],
            'Instructions': r['instructions'],
            'CookingTime': r['cookingTime'],
            'Keywords': r['keywords'],
            'NutritionValue': r['nutritionValue'],
            'Source': r['source'],
            'CookingTimeInMin': r['cookingTimeInMin'],
            'CalorieCount': r['calorieCount'],
            'IngredientsCount': r['ingredientsCount'] 
        }
        result.append(output)
    return jsonify(result)

if __name__ == '__main__':
    app.run(port=8000, debug=False)
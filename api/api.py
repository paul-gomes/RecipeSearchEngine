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
query = "tomato pasta"
#query = word_tokenize(query)

# for d_num in range(1, N):
#     d_dict = {}
#     d_ilist = []
#     for q in query:
#         try:
#             d = term_prox[str((str(d_num), q))]
#             d_dict[q] = d
#             d_ilist.append(d)
#             if (len(d_dict) == len(query)):
#                 smallest_span = 0
#                 all_comb = list(itertools.product(*d_ilist))
#                 for c in all_comb:
#                     a = np.sort(np.array(c))
#                     min_cover = (a[-1] - a[0])/ len(a)
#                     if smallest_span == 0 or min_cover < smallest_span :
#                         smallest_span = min_cover
#                 print(d_dict)
#         except:
#             break



# for d_num in range(1, N):
#     term_pair = list(itertools.combinations(query, 2))
#     pair_based_score = 0
#     for tp in term_pair:
#         try:
#             d_ilist = []
#             min_dist = 0
#             t1_positions = term_prox[str((str(d_num), tp[0]))]
#             t2_positions = term_prox[str((str(d_num), tp[1]))]
#             d_list = [t1_positions, t2_positions]
#             all_comb = list(itertools.product(*d_list))
#             for c in all_comb:
#                 dist = abs(c[0] - c[1])
#                 if min_dist == 0 or dist < min_dist:
#                     min_dist = dist
#             pair_based_score += 1/ pow(min_dist, 2)
#         except:
#             continue
        


# for d_num in range(1, N):
#     n = 0
#     for q in query:
#         d = term_prox[str((str(d_num), q))]
#         n += len(d)
#     print(n)
        

s_vector = doc_query_similarity(10, query, doc_vector, N, total_vocab, term_df)
print(s_vector)
result = []
for i in s_vector:
    r = data.loc[i]
    result.append(r)




s_vector_1 = doc_query_similarity_with_term_proximity(10, query, doc_vector, N, total_vocab, term_df, term_prox)
print(s_vector_1)
result1 = []
for i in s_vector_1:
    r = data.loc[i]
    result1.append(r)
    







































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
    query = "Chicken masala"
    s_vector = doc_query_similarity(50, query, doc_vector, N, total_vocab, term_df)
    print(s_vector)
    result = []
    for i in s_vector:
        r = data.loc[i]
        result.append(r)
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
    query = "Chicken masala"
    s_vector = doc_query_similarity(50, query, doc_vector, N, total_vocab, term_df)
    print(s_vector)
    result = []
    for i in s_vector:
        r = data.loc[i]
        result.append(r)
    return jsonify(result)
import pandas as pd
import nltk
import numpy as np
from nltk.tokenize import word_tokenize
from collections import Counter
import pickle
import math
import sys
sys.path.insert(0,"..")
from services.azure_cosmos import *
from services.azure_storage import *
from services.pre_process import *

def ConvertDictValueSetToList(dictSet):
    newDict = {}
    for key in dictSet:
        newDict[key] = list(dictSet[key])
    newDict.update({"id": '1'})
    return newDict 

def prepare_tfidf_for_json(dictSet):
    newDict = {}
    for key in dictSet:
        k = str(key)
        newDict[k] = dictSet[key]
    newDict.update({"id": '1'})
    return newDict  

def doc_frequency(word):
    try:
        return len(term_df[word])
    except:
        return 0



nltk.download('stopwords')
nltk.download('punkt')

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
query = "select * from RecipesContainer"
data = db.execute_query(recipe_container, query)


data_df = pd.DataFrame(data)
data_df = data_df[['id', 'title', 'ingredients', 'instructions', 'keywords']]
data_df = data_df.set_index('id')


#pre-process data
data_p = Pre_Process(data_df)
p_data = data_p.data_preprocess()


term_df = {}
#term_prox = {}
for index, row in p_data.iterrows():
    for col in p_data.columns:
        #pos = {}
        words = word_tokenize(str(row[col]))
        # for i in range(len(words)): # we need the positions for term proximity
        #     w = words[i]
        #     try:
        #         pos[w].append(i)
        #     except:
        #         pos[w] = [i]
        
        for w in words:
            try:
                term_df[w].add(index)
                #term_prox[str(index)+"__"+w].add(pos[w])
            except:
                term_df[w] = {index}
                #term_prox[str(index)+"__"+w] = pos[w]


term_df = ConvertDictValueSetToList(term_df)
#upload term document frequency in azure storage as json
#beacuse azure cosmos db throws error on request limit size exceed
storage.upload_dict_as_json(storage_container, term_df, 'term_df')


#Save term document frequency as a pickle file and in the database
with open('../Data/term-df.pickle', 'wb') as handle:
    pickle.dump(term_df, handle, protocol=pickle.HIGHEST_PROTOCOL)


# with open('../Data/term-prox.pickle', 'wb') as handle:
#     pickle.dump(term_prox, handle, protocol=pickle.HIGHEST_PROTOCOL)




total_vocab = [x for x in term_df]
N = len(p_data)

#TF-IDF for body/Instructions
tf_idf = {}
for index, row in p_data.iterrows():
    title = word_tokenize(str(row['title']))
    ingredients = word_tokenize(str(row['ingredients']))
    instructions = word_tokenize(str(row['instructions']))
    counter = Counter(title + ingredients + instructions )
    words_count = len(title + ingredients + instructions)

    for w in np.unique(instructions):
        tf = counter[w]
        df = doc_frequency(w)
        idf = np.log((N+1)/(df+1)) #to prevent division by 0
        tf_idf[index, w] = tf*idf


#TF-IDF for ingredients and title
tf_idf_title_ingredient = {}
N = len(p_data)
for index, row in p_data.iterrows():
    title = word_tokenize(str(row['title']))
    ingredients = word_tokenize(str(row['ingredients']))
    instructions = word_tokenize(str(row['instructions']))
    title_ingredients = title + ingredients
    counter = Counter(title + ingredients + instructions )
    words_count = len(title + ingredients + instructions)

    for w in np.unique(title_ingredients):
        tf = counter[w]
        df = doc_frequency(w)
        idf = math.log((N+1)/(df+1)) #to prevent division by 0
        tf_idf_title_ingredient[index, w] = tf*idf


#assigning more weigts to the words in Title and Ingredients
alpha = 0.8

for key in  tf_idf:
    tf_idf[key] *= alpha

for key in tf_idf_title_ingredient:
    tf_idf[key] = tf_idf_title_ingredient[key]
    
tf_idf = prepare_tfidf_for_json(tf_idf)

#upload tf-idf score in azure storage as json
#beacuse azure cosmos db throws error "request limit size exceeded"
storage.upload_dict_as_json(storage_container, tf_idf, 'tf_idf')

with open('../Data/tf-idf.pickle', 'wb') as handle:
    pickle.dump(tf_idf, handle, protocol=pickle.HIGHEST_PROTOCOL)



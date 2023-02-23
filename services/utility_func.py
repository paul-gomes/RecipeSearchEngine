from nltk.tokenize import word_tokenize
import numpy as np
from collections import Counter
import math
from services.pre_process import *
import itertools


def cosine_similarity(d,q):
    cos_sim = np.dot(d, q)/(np.linalg.norm(d)*np.linalg.norm(q))
    if math.isnan(cos_sim):
        return 0.0
    return cos_sim

def doc_frequency(word, term_df):
    try:
        return len(term_df[word])
    except:
        return 0
    
def gen_query_vector(query, N, total_vocab, term_df):
    words = word_tokenize(str(query))
    query_v = np.zeros((len(total_vocab)))
    counter = Counter(words)
    for w in np.unique(words):        
        tf = counter[w]
        df = doc_frequency(w, term_df)
        idf = math.log((N+1)/(df+1)) #to prevent division by 0
        try:
            index = total_vocab.index(w)
            query_v[index] = tf*idf
        except:
            pass
    return query_v

def expand_query_vector(query_v, relevant_docs_v, irrelevant_doc_v):
    alpha = 1
    beta = 0.75
    gamma = 0.15
    avg_r_v = np.mean(relevant_docs_v, axis=0)
    e_query = alpha * query_v + beta * avg_r_v - gamma * irrelevant_doc_v
    return e_query

def get_number_of_query_terms_in_document(query, term_prox, d_num):
    n = 0
    query = word_tokenize(query)
    for q in query:
        try:
            d = term_prox[str((str(d_num), q))]
            n += len(d)
        except:
            continue
    return n
            

#Span based approach
#The min_span distance in the span-based proximity approach will be calculated
#and then transformed to a span distance score
def calculate_smallest_span_score(query, term_prox, doc_num):
    term_positions = []
    query = word_tokenize(query)
    for q in query:
        try:
            positions_for_each_term = term_prox[str((str(doc_num), q))]
            term_positions.append(positions_for_each_term)
            #true when all query terms presents in the document
            if (len(term_positions) == len(query)):
                smallest_span = 0
                all_comb = list(itertools.product(*term_positions))
                for c in all_comb:
                    s = np.sort(np.array(c))
                    min_cover = (s[-1] - s[0])/ len(s)
                    if smallest_span == 0 or min_cover < smallest_span :
                        smallest_span = min_cover
                return 1/smallest_span**2 #transformed to a span distance score
        except:
            return 0 #for now if one of the query term isn't present, return 0
 

def calculate_pair_based_score(query, term_prox, doc_num):
    query = word_tokenize(query)
    term_pair = list(itertools.combinations(query, 2))
    pair_based_score = 0
    for tp in term_pair:
        try:
            d_ilist = []
            min_dist = 0
            t1_positions = term_prox[str((str(doc_num), tp[0]))]
            t2_positions = term_prox[str((str(doc_num), tp[1]))]
            d_list = [t1_positions, t2_positions]
            all_comb = list(itertools.product(*d_list))
            for c in all_comb:
                dist = abs(c[0] - c[1])
                if min_dist == 0 or dist < min_dist:
                    min_dist = dist
            pair_based_score +=  (1/ min_dist**2)
        except:
            continue
        
    return pair_based_score


def doc_query_similarity(k, query, doc_vector, N, total_vocab, term_df):
    p_query= Pre_Process(query).query_preprocess()
    d_cosines = []
    query_vector = gen_query_vector(p_query, N, total_vocab, term_df)
    for d in doc_vector:
        d_cosines.append(cosine_similarity(d, query_vector))

    out = np.array(d_cosines).argsort()[-k:][::-1]
    return out


def doc_query_similarity_with_term_proximity(k, query, doc_vector, N, total_vocab, term_df, term_prox):
    p_query= Pre_Process(query).query_preprocess()
    d_cosines_with_term_prox = []
    query_vector = gen_query_vector(p_query, N, total_vocab, term_df)

    for d in range(1, len(doc_vector)):
        # calculate cosine similarity
        cos_sim = cosine_similarity(query_vector, doc_vector[d-1])
       
        #total number of all query terms on document
        n = get_number_of_query_terms_in_document(p_query, term_prox, d)
        
        if n > 0:
            #calculate the smallest span in the document for the given query
            span = calculate_smallest_span_score(p_query, term_prox, d)            
            #calculate aggregated pair based score
            aggregated_pair_based_score = calculate_pair_based_score(p_query, term_prox, d)
            term_proximity = (aggregated_pair_based_score + span) / n
        else:
            term_proximity = 0
        
        rsv = cos_sim + term_proximity
        
        d_cosines_with_term_prox.append(rsv)

    out = np.array(d_cosines_with_term_prox).argsort()[-k:][::-1]
    return out


def doc_query_similarity_with_query_expansion(k, query, doc_vector, N, total_vocab, term_df, relevant_docs_v, irrelevant_doc_v):
    p_query= Pre_Process(query).query_preprocess()
    d_cosines = []
    query_vector = gen_query_vector(p_query, N, total_vocab, term_df)
    e_query_vector = expand_query_vector(query_vector, relevant_docs_v, irrelevant_doc_v)


    for d in range(len(doc_vector)):
        # calculate cosine similarity
        val = cosine_similarity(e_query_vector, doc_vector[d])
        d_cosines.append(val)

    out = np.array(d_cosines).argsort()[-k:][::-1]
    return out

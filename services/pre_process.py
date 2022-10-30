import pandas as pd
import string
import nltk
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from collections import Counter
import pickle

class Pre_Process:
    def __init__(self, data):
        self.data = data 
        
    def convert_to_lower(self, text):
        return text.lower()

    
    def remove_symbols(self, text):
        words = text
        words = words.translate(str.maketrans('', '', string.digits))
        words = words.translate(str.maketrans('', '', string.punctuation))  
        return words
    
    def remove_stop_words(self, text):
        stop_words = stopwords.words('English')
        words = word_tokenize(text)
        new_words = ""
        for w in words:
            if w not in stop_words and len(w) > 1:
                new_words = new_words + " " + w     
        return new_words


    def porter_stemming(self, text):
        stemmer = PorterStemmer()
        words = word_tokenize(text)
        new_words = ""
        for w in words:
            new_words = new_words + " " + stemmer.stem(w)
        return new_words
    
    def data_preprocess(self):              
        data = self.data.copy()
        for index, row in data.iterrows():
            for col in data.columns:
                words = str(row[col])
                new_words = self.convert_to_lower(words)
                new_words = self.remove_symbols(new_words)
                new_words = self.remove_stop_words(new_words)
                new_words = self.porter_stemming(new_words)
                data.at[index, col]  = new_words
        return data
    
    def query_preprocess(self):
        query = self.data
        p_query = self.convert_to_lower(query)
        p_query = self.remove_symbols(p_query)
        p_query = self.remove_stop_words(p_query)
        p_query = self.porter_stemming(p_query)
        return p_query

# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 20:52:34 2015

@author: root
"""

import argparse

from pyspark import SparkConf
from pyspark import SparkContext
from collections import defaultdict
from itertools import chain, combinations

#A general function to print the RDD item
def print_line(val):
    print(val)
    
#Print all the values of a RDD item tuple
def print_line_vals(line):
    for i in line[1]:
        print i[0]

#The map method used to pre-process the initial raw customer data and convert into RDD tuples
def map_line(line): 
    
    tokens = line.split(';')
    
    if len(tokens) ==  3:    
        
        timestamp = int(tokens[0])
        userid = int(tokens[1])
        productid = tokens[2]
        return (userid, (productid, timestamp))
        
    else:
        
        return (-1, (str(-1), 0)) 
        
#This method calculates the tanimoto coefficient between two customers   
def calc_similarity(val, user_vals):
    
    user_id = val[0]
    
    user_items = [v[0] for v in val[1]]
    user_items = set(user_items)
    
    sel_user_items = [v[0] for v in user_vals]
    sel_user_items = set(sel_user_items)
        
    items_common = [x for x in user_items if x in sel_user_items]

    if len(items_common) == 0:
        return (user_id, val[1], 0.0)
    else:
        tanimoto_coeff = len(items_common) / ((len(user_items) + len(sel_user_items) - len(items_common)) * 1.0)
        return (user_id, val[1], tanimoto_coeff)
        
#This method groups all the transactions by a customer by timestamp
def groupByTimeStamp(line):
    
    v = defaultdict(list)

    for key_value_pair in line[1]:
        if not key_value_pair[0] in v[key_value_pair[1]]:
            v[key_value_pair[1]].append(key_value_pair[0])

    return (line[0], v, line[2])
    
#This method returns those association rules that have a minimum support
def returnItemsWithMinSupport(itemSet, transactionList, minSupport, freqSet):
        """calculates the support for items in the itemSet and returns a subset
       of the itemSet each of whose elements satisfies the minimum support"""
        _itemSet = set()
        localSet = defaultdict(int)

        for item in itemSet:
                for transaction in transactionList:
                        if item.issubset(transaction):
                                freqSet[item] += 1
                                localSet[item] += 1

        for item, count in localSet.items():
                support = float(count)/len(transactionList)

                if support >= minSupport:
                        _itemSet.add(item)
        
        return _itemSet


def subsets(arr):
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])
    
def joinSet(itemSet, length):
    """Join a set with itself and returns the n-element itemsets"""
    return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])
        
#This method runs the association rules on top of all the transactions for a particular user
def calc_confidence(line, minSupport, minConfidence):
    
    user_similarity = line[2]
    transactionList = list()
    itemSet = set()
    
    for key in dict(line[1]):
        #print line[1]
        transaction = frozenset(list(dict(line[1])[key]))
        transactionList.append(transaction)
        for item in transaction:
            itemSet.add(frozenset([item]))
            
    freqSet = defaultdict(int)
    largeSet = dict()
    # Global dictionary which stores (key=n-itemSets,value=support)
    # which satisfy minSupport

    #assocRules = dict()
    # Dictionary which stores Association Rules
    
    oneCSet = returnItemsWithMinSupport(itemSet,
                                        transactionList,
                                        minSupport,
                                        freqSet)
                                        
    currentLSet = oneCSet
    k = 2
    
    while(currentLSet != set([])):
        largeSet[k-1] = currentLSet
        currentLSet = joinSet(currentLSet, k)
        
        
        _itemSet = set()
        localSet = defaultdict(int)
        
        for item in currentLSet:
                for transaction in transactionList:
                        if item.issubset(transaction):
                                freqSet[item] += 1
                                localSet[item] += 1
        
        if len(localSet) > 0:
            for item, count in localSet.items():
                    
                    if len(transactionList) == 0:
                        support = 0.0;
                        
                    support = float(count)/len(transactionList)
    
                    if support >= minSupport:
                            _itemSet.add(item)
        
        currentCSet = _itemSet
        currentLSet = currentCSet
        k = k + 1    

    def getSupport(item):
            """local function which Returns the support of an item"""
            return float(freqSet[item])/len(transactionList)
            
    toRetItems = []
    for key, value in largeSet.items():
        toRetItems.extend([(tuple(item), getSupport(item))
                           for item in value])

    toRetRules = []
    if len(largeSet) < 2:
        return (line[0], toRetRules)        
        
    for key, value in largeSet.items()[1:]:
        for item in value:
            _subsets = map(frozenset, [x for x in subsets(item)])
            for element in _subsets:
                remain = item.difference(element)
                if len(remain) > 0:
                    confidence = getSupport(item)/getSupport(element)
                    if confidence >= minConfidence:
                        toRetRules.append(((tuple(element), tuple(remain)),
                                           confidence*user_similarity))
    
    return (line[0], toRetRules)  

#This method filters the final set of rules such that the products being predicted are not among the ones already purchased by the user
def filter_rules(line, user_vals):
    
    rule = line

    source_items = rule[1][0]
    pred_items = rule[1][1]
    
    user_items = [v[0] for v in user_vals]
    user_items = set(user_items) 
    
    not_in_user_source = []
    in_user_pred = []
    not_in_user_source = [v for v in source_items if v not in user_items]
    in_user_pred = [v for v in pred_items if v in user_items]    
    
    if (len(not_in_user_source) == 0) and (len(in_user_pred) == 0):
        return True
        

    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = """
    A recommendation system to recommend a maximum of 5 products for a user input""")
   
    parser.add_argument('-user_id', type=int, required=True,
                        help="The id of the user to recommend a maximum of 5 products")
                        
    parser.add_argument('-file_path', type=str, required=True,
                        help='The complete path of the csv file containing the metadata in form of "timestamp;userid;productid"')
    
    args = vars(parser.parse_args())
    
    print 'Arguments used: ' + str(args)
    
    user_id   = args['user_id']
    file_path = args['file_path']    
    
    conf = SparkConf().setAppName("Product Recommendation").setMaster("local")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.python.worker.memory", "4g")
    
    sc = SparkContext(conf=conf)
    
    userid = user_id
    
    support = 0.0
    confidence = 0.0
    num_similar_users = 800 #the number of similar users to consider
    
    rating_data_raw = sc.textFile(file_path, minPartitions=100)
    
    rating_data_kv = rating_data_raw.map(map_line)
    rating_data_kv_grouped = rating_data_kv.groupByKey()
    rating_data_kv_grouped_clean = rating_data_kv_grouped.filter(lambda line: line[0] != -1)
    rating_data_kv_grouped_clean.cache()    
    
    rating_data_kv_grouped_user = rating_data_kv_grouped_clean.filter(lambda line: line[0] == userid)
    print ("The products already purchased are")
    rating_data_kv_grouped_user.foreach(print_line_vals)
    
    user_kv = rating_data_kv_grouped_user.take(1)
    user_vals = user_kv[0][1]
    
    rating_data_kv_grouped_other                            = rating_data_kv_grouped_clean.filter(lambda line: line[0] != userid)

    user_similarity_rdd                                     = rating_data_kv_grouped_other.map(lambda line: calc_similarity(line, user_vals))
    
    user_similarity_sel                                     = user_similarity_rdd.filter(lambda line: line[2] > 0.0)    

    user_similarity_timestamps                              = user_similarity_sel.map(lambda line: groupByTimeStamp(line))

    user_similarity_timestamps_sort                         = user_similarity_timestamps.sortBy(lambda line: line[2], ascending=False).take(num_similar_users)

    user_similarity_timestamps_sort_rdd                     = sc.parallelize(user_similarity_timestamps_sort, 200)
    
    user_similarity_timestamps_confidence                   = user_similarity_timestamps_sort_rdd.map(lambda line: calc_confidence(line, support, confidence))

    user_similarity_timestamps_confidence_filtered          = user_similarity_timestamps_confidence.filter(lambda line: len(line[1]) > 0)

    confidence_rules                                        = user_similarity_timestamps_confidence_filtered.flatMap(lambda line: [(v[1],v[0]) for v in line[1]])
    
    confidence_rules_filtered                               = confidence_rules.filter(lambda line: filter_rules(line, user_vals))
    
    confidence_rules_filtered.cache()
    
    final_rules                                             = confidence_rules_filtered.sortByKey(ascending=False)

    final_recommendations                                   = final_rules.flatMap(lambda line: [v for v in line[1][1]]).take(5)

    print("The final set of recommendations are")
    print(final_recommendations)

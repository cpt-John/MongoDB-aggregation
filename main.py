# pip install dnspython
# pip install pymongo[srv]

from pymongo import MongoClient
import json


def db_init(db_name, collection_name):
    fileName = "db_link.txt"
    db_link = ''
    try:
        file = open(fileName, "r")
        db_link = file.read()
        file.close()
    except:
        pass
    link_exists = bool(db_link)
    if not link_exists:
        db_link = input("enter mongodb connection url: ")
        file = open(fileName, "w")
        file.write(db_link)
        file.close()
    client = ''
    try:
        client = MongoClient(db_link)
        db = client[db_name]
        return [db, db[collection_name]]
    except Exception as e:
        print("db connection failed")
        raise Exception(e)


def new_collection(collection_name, data_array, db_client,):
    if not len(data_array):
        print("Empty data!")
        return False
    db_conn = db_client[collection_name]
    db_conn.delete_many({})
    db_conn.insert_many(data_array)
    print("Inserted!")
    return true


def load_data_to_db(data_file_path, db_conn):
    data_file = open(data_file_path)
    data_array = json.load(data_file)
    db_conn.delete_many({})
    db_conn.insert_many(data_array)
    print("DB ready!")
    return true


def map_helper(iterator, processor=lambda x: x): return [
    processor(i) for i in iterator]


def main():
    # innit db
    DB_CLIENT, DB_CONN = db_init('student_database', 'full_data')

    # loading data to DB
    load_data_to_db('./students.json', DB_CONN)

    pass_score = 40

    # Get max scores
    max_scores_iterator = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', }},
        {'$group': {"_id": "$type",
         'maxScore': {'$max': "$score"}, }},
        {'$project':
         {'score': '$maxScore', 'type': '$_id', '_id': 0}},
    ])
    max_scores = map_helper(max_scores_iterator)
    result = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', 'name': 1, '_id': 0}},
        {"$match":
         {"$or":
          map_helper(max_scores, lambda x:  {
              "score": x['score'], "type":x['type']})
          }
         }
    ])
    result = map_helper(result)
    print('\n ******** Max Scores \n')
    print(result)
    print('\n ******** \n')

    # Get below avg scores
    avg_score_iterator = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', }},
        {
            '$match': {"type": "exam"}
        },
        {'$group': {"_id": "$type",
         'avgScore': {'$avg': "$score"}, }},
        {'$project':
         {'score': '$avgScore', 'type': '$_id', '_id': 0}},
    ])
    avg_score = map_helper(avg_score_iterator)[0]
    result = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', 'name': 1, '_id': 0}},
        {"$match": {
            "score": {"$lt": avg_score['score']}, "type":avg_score['type']}
         }
    ])
    result = map_helper(result)
    print('\n ********Below Avg Scores \n')
    print(result)
    print('\n ******** \n')

    # Get status & scores
    status_score_iterator = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score',
          'name': 1
          }},
        {'$project':
            {
                'name': 1,
                'status':
                {
                    '$cond': {'if': {'$gte': ["$score", pass_score]}, 'then': 'pass', 'else': 'fail'}
                },
                'type':1, 'score':1, '_id':0
            },
         },
    ])
    result = map_helper(status_score_iterator)
    print('\n ******** Status Scores \n')
    print(result)
    print('\n ******** \n')

    # Get total & avg scores
    total_avg_score_iterator = DB_CONN.aggregate([
        {'$project':
            {
                '_id': 1,
                'name': 1,
                'total': {'$sum': '$scores.score'},
                'average': {'$avg': '$scores.score'},
            },
         },
    ])
    result = map_helper(total_avg_score_iterator)
    print('\n ******** Total & Avg Scores \n')
    print(result)
    print('\n ******** Updating into Collection... \n')
    new_collection('total_and_avg', result, DB_CLIENT)

    # Get below avg pass-all scores
    avg_score_iterator = DB_CONN.aggregate([
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', }},
        {'$group': {"_id": "$type",
         'avgScore': {'$avg': "$score"}, }},
        {'$project':
         {'score': '$avgScore', 'type': '$_id', '_id': 0}},
    ])

    avg_score = map_helper(avg_score_iterator)

    def processor(x):
        return {
            "score": {"$lt": x['score'], "$gte": pass_score}, "type": x['type']}

    match_qry = map_helper(avg_score, processor)
    student_names = DB_CONN.aggregate([
        {'$project':
         {'id': '$_id', 'name': 1, 'scores': 1}},
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', 'id': 1}},
        {"$match": {"$or":
                    match_qry}
         },
        {
            '$group': {'_id': '$id', 'count': {'$sum': 1}},
        },
        {"$match": {"count": {"$gte": len(avg_score)}}
         },
    ])
    sub_qry = map_helper(student_names, lambda x: x['_id'])
    result = map_helper(DB_CONN.find({"_id": {'$in': sub_qry}}))
    print('\n ********Below Avg Pass in All Scores \n')
    print(result)
    print('\n ******** Updating into Collection... \n')
    new_collection('below_avg_pass_all', result, DB_CLIENT)

    # Get fail-all scores

    categories = [x['type']for x in (DB_CONN.find_one({}))['scores']]

    def processor(x):
        return {
            "score": {"$lt": pass_score}, "type": x}

    match_qry = map_helper(categories, processor)
    student_names = DB_CONN.aggregate([
        {'$project':
         {'id': '$_id', 'name': 1, 'scores': 1}},
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', 'id': 1}},
        {"$match": {"$or":
                    match_qry}
         },
        {
            '$group': {'_id': '$id', 'count': {'$sum': 1}},
        },
        {"$match": {"count": {"$gte": len(categories)}}
         },
    ])
    sub_qry = map_helper(student_names, lambda x: x['_id'])
    result = map_helper(DB_CONN.find({"_id": {'$in': sub_qry}}))
    print('\n ********Below fail in all scores \n')
    print(result)
    print('\n ******** Updating into Collection... \n')
    new_collection('fail_all', result, DB_CLIENT)

    # Get pass-all scores

    categories = [x['type']for x in (DB_CONN.find_one({}))['scores']]

    def processor(x):
        return {
            "score": {"$gte": pass_score}, "type": x}

    match_qry = map_helper(categories, processor)
    student_names = DB_CONN.aggregate([
        {'$project':
         {'id': '$_id', 'name': 1, 'scores': 1}},
        {'$unwind': "$scores"},
        {'$project':
         {'type': '$scores.type',
          'score': '$scores.score', 'id': 1}},
        {"$match": {"$or":
                    match_qry}
         },
        {
            '$group': {'_id': '$id', 'count': {'$sum': 1}},
        },
        {"$match": {"count": {"$gte": len(categories)}}
         },
    ])
    sub_qry = map_helper(student_names, lambda x: x['_id'])
    result = map_helper(DB_CONN.find({"_id": {'$in': sub_qry}}))
    print('\n ******** Pass all scores \n')
    print(result)
    print('\n ******** Updating into Collection... \n')
    new_collection('pass_all', result, DB_CLIENT)

    print("Completed!")


main()  # Execution starts here

import json
from bson import json_util
from flask import Flask, request, make_response
from flask_pymongo import PyMongo
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)
app.debug = True
app.config["MONGO_URI"] = "mongodb://localhost:27017/impdb"
mongo = PyMongo(app)

class Impressions(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('domain', type=str, help='Domain name is required', required=True)
        args = parser.parse_args()
        impressions_list = list(mongo.db.impressions.find({"headers.Referer": {"$regex": args['domain']}}))
        creative_sizes = []
        for i in impressions_list:
            try:
                creative_sizes.append('{}x{}'.format(i['ad_width'], i['ad_height']))
            except:
                pass
            try:
                creative_sizes.append(i['creative_size'])
            except:
                pass
        result_json = json.dumps({'data': impressions_list, 'creative_sizes': creative_sizes}, default=json_util.default)
        resp = make_response(result_json)
        resp.mimetype = "application/json"
        return resp

    def post(self):
        impressions = mongo.db.impressions
        # Strange behaviour of reqparser for proper parsing of json, using flask's request instead
        json_data = request.get_json()
        impression_id = json_data.get('impression_id')
        # Not doing any input validation really as the schema varies
        # Rather, if the impression_id exists, checking the collection for the record with this id
        # and performing upsert in order not to duplicate the data
        if not impression_id:
            impressions.insert(json_data)
        else:
            impressions.update({"impression_id": impression_id}, json_data, upsert=True)

        result = {'status': 'ok'}
        resp = make_response(json.dumps(result), 201)
        resp.mimetype = "application/json"
        return resp

api.add_resource(Impressions, '/api/v1/impressions')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

from datetime import datetime
from datetime import timedelta
import base64
import requests
import json
import copy
import argparse
import sys

def define_args():
    parser = argparse.ArgumentParser(description='kibana log processing')
    parser.add_argument('--api', type=str, help='api..example /QueryParser/v1/parse_token',
            required=False, default="")
    parser.add_argument('--haproxy', type=str, default="dev-haproxy-*", required=False,
            help='comma seperated haproxy list dev-haproxy-*,previewalb-haproxy-*')
    parser.add_argument('--past_days', type=int,
            help='past_days', default=1, required=False)
    parser.add_argument('--size', type=int,
            help='size', default=-1, required=False)
    parser.add_argument('--output', type=str,
            help='output file name', default='output.json', required=False)

    args = parser.parse_args()
    return args

def kibana_req_body(api, start_date, end_date, last_sort_value, size=10000):
    input_data = 
    if api!="":
        more_input = {
            "match_phrase": {
                "endpoint": {
                        "query": f"{api}"
                }
            }
        }
        input_data["query"]["bool"]["filter"].append(more_input)
    # if last_sort_value != 0:
    #     input_data["search_after"]=[last_sort_value]

    return input_data

def get_kibana_response(api, start_date, end_date, haproxy, size, last_sort_value):
    basic_auth = f"harish@nference.net:8b711abdab60298e97d8d61e2f998b49"
    sig = base64.standard_b64encode(basic_auth.encode('utf-8')).decode('utf-8')

    kibana_url = f"https://preview.nferx.com/kibana/elasticsearch/{haproxy}/_search?rest_total_hits_as_int=true&ignore" \
                 "_unavailable=true&ignore_throttled=true&preference=1608621173425&timeout=30000ms"
    input_data = kibana_req_body(api, start_date, end_date, last_sort_value, size=size)

    headers = {'kbn-version': '7.5.1', 'Authorization': f"Basic {sig}"}
    response = (requests.post(url=kibana_url, json=input_data, headers=headers)).json()

    with open("response.json", "w") as f:
        json.dump(response, f)
    
    sys.exit(0)

    return response

def get_query_list(response):
    hits = []
    data={}

    if 'hits' in response.keys() and 'hits' in response['hits'].keys():
        hits = response['hits']['hits']
        for hit in hits:
            if '_source' in hit.keys() and 'query' in hit['_source'].keys():
                if hit['_source']['endpoint'] not in data:
                    data[hit['_source']['endpoint']]={}
                    data[hit['_source']['endpoint']][hit['_source']['query']]=1
                    
                else:
                    if hit['_source']['query'] not in data[hit['_source']['endpoint']]:
                        data[hit['_source']['endpoint']][hit['_source']['query']]=1
                    else:
                        data[hit['_source']['endpoint']][hit['_source']['query']]=data[hit['_source']['endpoint']][hit['_source']['query']]+1

    return data

def get_merged_response(base_response, new_response):
    for endpoint, queries in new_response.items():
        if endpoint in base_response.keys():
            for query, count in queries.items():
                if query in base_response[endpoint]:
                    base_response[endpoint][query] += count
                else:
                    base_response[endpoint][query] = count
        else:
            base_response[endpoint] = queries

    return base_response

def get_unique_queries_from_kibana(args, haproxy_list=[]):
    end_date = datetime.now()
    delta = timedelta(args.past_days)
    start_date = end_date - delta
    start_date = start_date.isoformat()
    end_date = end_date.isoformat()
    data={}
    final_data = {}
    total_hits = -1
    for haproxy in haproxy_list:
        size=args.size
        last_sort_value = 0
        is_size_specified = True
        if size == -1:
            is_size_specified = False
            size = 10000
        
        total_hits = -1
        first_pass = True
        iteration = 1
        while (is_size_specified and size > 0) or (not is_size_specified and
                total_hits > 0) or first_pass:
            if is_size_specified:
                if size >= 10000:
                    size_pass = 10000
                else:
                    size_pass = size
            elif total_hits >= 10000 or first_pass:
                size_pass = 10000
            else:
                size_pass = total_hits
           
            print(iteration)
            iteration = iteration + 1
            response = get_kibana_response(args.api, start_date, end_date, haproxy, size_pass, last_sort_value)
            if 'hits' in response.keys() and 'hits' in response['hits'].keys():
                hits = response['hits']['hits']
                if len(hits) > 0 and 'sort' in hits[len(hits)-1] and len(hits[len(hits)-1]['sort']) > 0:
                    last_sort_value = hits[len(hits)-1]['sort'][0]
                if total_hits == -1:
                    total_hits = response['hits']["total"]
            size = size - size_pass
            total_hits = total_hits - size_pass
            query_data = get_query_list(response)
            final_data = get_merged_response(final_data, query_data)
            first_pass = False

        data[haproxy] = final_data

    return data

def main(args):
    haproxy_list = args.haproxy.split(",")
    data = get_unique_queries_from_kibana(args, haproxy_list)
    result = []
    with open(args.output, 'w') as of:
        for haproxy, values in data.items():
            output = {}
            output["haproxy"] = haproxy
            for endpoint, queries in values.items():
                #of.write("%s\t%s\t%s\n"% (haproxy, endpoint, "\n".join(queries)))
                output["endpoint"] = endpoint
                output["queries"] = []
                for query in queries:
                    output["queries"].append({"query":query,"count":queries[query]})
                result.append(copy.deepcopy(output))
        of.write(json.dumps(result, indent=2))
        of.write("\n")

if __name__ == "__main__":
    args = define_args()
    main(args)

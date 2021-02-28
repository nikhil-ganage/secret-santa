from flask import Flask,flash, render_template, request      
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine
import random
from elasticsearch import Elasticsearch
from elasticsearch import helpers

app = Flask(__name__,template_folder='template')
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
app._static_folder = 'static'
es = Elasticsearch(['https://vpc-nikhil-tenable-es-dujxdvsp2xxj7fjnekl5mg5k2e.eu-west-1.es.amazonaws.com',])

@app.route("/home", methods=['GET', 'POST'])
def home():
    if request.method == "POST":
        details = request.form
        result2=details.to_dict(flat=False)

        output = []
        for i in range(len(result2['first_name'])):
            di = {}
            for key in result2.keys():
                di[key] = result2[key][i]
            output.append(di)

        df = pd.json_normalize(output)
        df = df[df['first_name'].astype(bool)]  
        family_hash = hash(df.head(1).to_string(header=False,index=False,index_names=False))
        df['family_hash'] = family_hash 
        conn = create_engine('postgresql://awsuser:LightsOut007!!@nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com:5439/dev')
        df.to_sql('family_member_names', conn, index=False, if_exists='append')
        return render_template("home.html", message=details);
    return render_template("home.html"); 

@app.route("/member", methods=['GET', 'POST'])
def member():
    if request.method == "POST":
        details = request.form
        result2=details.to_dict(flat=False)

        output = []
        for i in range(len(result2['first_name'])):
            di = {}
            for key in result2.keys():
                di[key] = result2[key][i]
            output.append(di)

        df = pd.json_normalize(output)
        conn = create_engine('postgresql://awsuser:LightsOut007!!@nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com:5439/dev')
        df.to_sql('family_member_names', conn, index=False, if_exists='append')
        return render_template("member.html", message=details);
    return render_template("member.html"); 

@app.route("/member_display", methods=['GET', 'POST'])
def member_display():
    if request.method == "POST":
        conn = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
        mycursor =conn.cursor()
        mycursor.execute("select * from family_member_names")
        data = mycursor.fetchall()
        return render_template("member_display.html", data=data);
    return render_template("member_display.html");

@app.route("/delete", methods=['GET', 'POST'])
def delete():
    if request.method == "POST":
        del_type = request.form.get('del')
        if del_type == "secretsanta":
            conn = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
            mycursor =conn.cursor()
            mycursor.execute("begin;")
            mycursor.execute("delete from secret_santa")
            mycursor.execute("commit;")
            indices = ['nikhil-tenable']
            es.delete_by_query(index=indices, body={"query": {"match_all": {}}})
            return render_template("delete.html", data="Deleted Secret Santa draw data");
        else:
            conn = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
            mycursor =conn.cursor()
            mycursor.execute("begin;")
            mycursor.execute("delete from secret_santa")
            mycursor.execute("commit;")
            return render_template("delete.html", data="Deleted all members data");
    return render_template("delete.html");

@app.route("/santa_display", methods=['GET', 'POST'])
def santa_display():
    if request.method == "POST":
        processing_year = request.form.get('year')
        conn = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
        mycursor =conn.cursor()
        mycursor.execute("select * from secret_santa where year ="+processing_year)
        data = mycursor.fetchall()
        return render_template("santa_display.html", data=data);
    if request.method == "GET":
        processing_year = request.form.get('year')
        conn = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
        mycursor =conn.cursor()
        mycursor.execute("select * from secret_santa order by receiver_member_id")
        data = mycursor.fetchall()
        return render_template("santa_display.html", data=data);
    return render_template("santa_display.html");

@app.route("/santagen", methods=['GET', 'POST'])
def santagen():
    if request.method == "POST":
        processing_year = request.form.get('year')
        connstr= 'postgresql://awsuser:LightsOut007!!@nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com:5439/dev'
        conn = create_engine(connstr)
        member_df = pd.read_sql("""select * from family_member_names""", conn)
        #res = es.search(index="nikhil-tenable", body={"query": {"match": {'year':processing_year}}})
        conn2 = psycopg2.connect(dbname='dev', host='nikhil-tenable.cawjpdsteduj.eu-west-1.redshift.amazonaws.com', port='5439', user='awsuser', password='LightsOut007!!')
        mycursor =conn2.cursor()
        mycursor.execute("select * from secret_santa where year="+processing_year+" limit 1")
        data = mycursor.fetchall()
        if not data:
            santa_df = genSecretSanta(member_df,processing_year)
            tries = santa_df.tries[1]
            santa_df = santa_df.drop("tries", axis=1)
            if tries < 100:
                santa_df.columns = ['receiver_member_id', 'giver_member_id', 'year', 'r_g_y_hash']
                santa_df.to_sql('secret_santa', conn, index=False, if_exists='append')
                helpers.bulk(es, doc_generator(santa_df))
                return render_template("santagen.html", message=santa_df);
            else:
                return render_template("santagen.html", message="WARNING! UNABLE TO DRAW SECRET SANTA. TRY AGAIN OR ADD MORE MEMBERS");
        else:
            return render_template("santagen.html", message="WARNING! ALREADY PROCESSED FOR THIS YEAR");
    return render_template("santagen.html");


use_these_keys = ['receiver_member_id', 'giver_member_id', 'year', 'r_g_y_hash']
def filterKeys(document):
    return {key: document[key] for key in use_these_keys }

def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": 'nikhil-tenable',
                "_type": "secretsanta",
                "_id" : f"{document['r_g_y_hash']}",
                "_source": filterKeys(document),
            }
    raise StopIteration

def genSecretSanta(member_df,processing_year, *args):
    givers = member_df['member_id'].tolist()
    santa_df = pd.DataFrame(columns=['receiver','giver'])
    member_dict = member_df.set_index('member_id')['family_hash'].to_dict()
    tries=0;
    tries_max = 100;
    result = []
    restart = True
    previous_year =int(processing_year) - 1
    previous_2_year =int(processing_year) - 2
    while restart:
        print(restart)  
        restart = False
        receivers = givers[:]
        for i in range(len(givers)):
            giver = givers[i]
            # Assign random reciever
            receiver = random.choice(receivers)
            # restart the generation once If we've got to the last giver and its the same as the reciever, 
            if (giver == receiver and i == (len(givers) - 1)):
                restart = True
                print("Break")
                break
            else:
                local_hash = hash(str(receiver)+ str(giver))
                res = es.search(index="nikhil-tenable", body={"query": {"match": {'r_g_y_hash':local_hash}}})
                if res['hits']['total']['value'] != 0:
                    es_year = res['hits']['hits'][0]['_source']['year']
                else:
                    es_year = 0
                print(res)
                print(es_year)
                # Ensure the giver and reciever are not the same, and they are not from the same family and they have not been paired in the last 3 years
                while ((receiver == giver) or (member_dict[giver] == member_dict[receiver]) or 
                    (int(es_year) == previous_year or int(es_year) == previous_2_year or int(es_year) == int(processing_year) )) and tries <tries_max:
                    receiver = random.choice(receivers)
                    # Generate pair hash value
                    local_hash = hash(str(receiver)+ str(giver))
                    # Get hash query result from the Elasticsearch
                    res = es.search(index="nikhil-tenable", body={"query": {"match": {'r_g_y_hash':local_hash}}})
                    # If present get the year of secret santa assignment else 0
                    if res['hits']['total']['value'] != 0:
                        es_year = res['hits']['hits'][0]['_source']['year']
                    else:
                        es_year = 0
                    print(res)
                    print(es_year)
                    tries = tries+1
                    print(tries)
                result.append(str(giver) + ' is buying for ' + str(receiver))
                santa_df.loc[i] = [receiver, giver]
                #Remove from the list
                receivers.remove(receiver)
                print(receivers)
    for r in result:
        print(r)
    santa_df['year'] =  processing_year
    santa_df["r_g_y_hash"] = (santa_df["receiver"].astype(str) + santa_df["giver"].astype(str)).apply(hash)
    santa_df['tries'] =  tries
    return santa_df

if __name__ == "__main__":    
    app.run(host='0.0.0.0',port=8080)


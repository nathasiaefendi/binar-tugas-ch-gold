import sqlite3
import pandas as pd
import re,string
import seaborn as sns
import scipy.stats.distributions as dist
import sqlite3

from flask import Flask, request, jsonify, send_file
from flasgger import Swagger, LazyString, LazyJSONEncoder, swag_from
from cleantext import clean
from matplotlib import pyplot as plt
from functools import reduce
from cgitb import text
from datetime import datetime



app = Flask(__name__)

app.json_encoder = LazyJSONEncoder

swagger_template = dict(
    info = {
        'title': LazyString(lambda: 'API Documentation for Text Cleansing'),
        'version' : LazyString(lambda : '1.0.0'),
        'description' : LazyString(lambda : 'Dokumentasi API untuk Text Cleansing. <br> text-cleansing : normal cleansing <br> text-cleansing-advanced : menghilangkan stopwords dan slang'),
    },
    host = LazyString(lambda : request.host)
)

swagger_config = {
    'headers': [],
    'specs': [
        {
            'endpoint': 'docs',
            'route': '/docs.json',
        }
    ],
    'static_url_path': "/flasgger_static",
    'swagger_ui': True,
    'specs_route': "/docs/"
}

swagger = Swagger(app, template=swagger_template, config = swagger_config)


#pipeline function untuk memudahkan penggunaan function
def pipeline_function(data, fns):
    return reduce(lambda a, x: x(a),
                  fns,
                  data)

#text file helper : slang & stopwords
#import data slang
slang_list = pd.read_csv('file-helper/new_kamusalay.csv', delimiter = ",", encoding='latin-1', header=None)
slang_list.columns = ['slang', 'meaning']

#import data stopwords
stopword_list = open('file-helper/stopwords.txt','r').read().splitlines()

###Text cleansing function
#text cleansing based on twitter hatespeech. It cleanse links, retweet, user, mentions, hashtags, emoji, resulting only the alphabet on the text

#strip links/hyperlink(if exist) 
def strip_links(text):
    link_regex    = re.compile('((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)', re.DOTALL)
    links         = re.findall(link_regex, text)
    for link in links:
        text = text.replace(link[0], ', ')    
    return text

#strip mentions and hashtags
def strip_all_entities(text):
    entity_prefixes = ['@','#']
    for separator in  string.punctuation:
        if separator not in entity_prefixes :
            text = text.replace(separator,' ')
    words = []
    for word in text.split():
        word = word.strip()
        if word:
            if word[0] not in entity_prefixes:
                words.append(word)
    return ' '.join(words)

#strip retweet
def remove_rt_user(text):
    text = text.replace('rt', '')
    text = text.replace('user', '')
    return text

#remove emoji
def remove_emoji(text):
    return clean(text, no_emoji=True)

#remove other things other than what has been cleansed by functions above
def alphabet_only(text):
    return re.sub(r'[^a-zA-Z]', ' ', text)

##Advanced cleansing:
#remove unnecesary word, stopwords and slang to get only the meaningful word based on the text

def replace_slang(text):
    text_list = text.split()
    cleansed_text = []
   
    for word in text_list:
      used_word = word
      for i, slang in enumerate(slang_list.slang):
        if slang == word:
          used_word = slang_list.meaning[i]
      cleansed_text.append(used_word)
    return ' '.join(cleansed_text)

def replace_stopwords(text):
    text_list = text.split()
    cleansed_text = [text for text in text_list if not any(word in text for word in stopword_list)]
    return ' '.join(cleansed_text)

def clean_text(text):
    text = str(text).lower()
    return pipeline_function(text, [
                     remove_rt_user,
                     remove_emoji,
                     strip_links,
                     strip_all_entities,
                     alphabet_only])

def clean_text_advanced(text):
    text = str(text).lower()
    return pipeline_function(text, [
                     clean_text,
                     replace_slang,
                     replace_stopwords])


#database function
def save_to_sqllite(raw, output, advanced_output = ''):
    try:
        query = ''

        if(advanced_output == ''):
            query = "INSERT INTO text_cleansing (raw, basic_output) VALUES ('{0}', '{1}')".format(raw, output)
        else:
            query = "INSERT INTO text_cleansing (raw,basic_output,advanced_output) VALUES ('{0}', '{1}', '{2}')".format(raw, output, advanced_output)
            # query = 'INSERT INTO text_cleansing (raw, output, advanced_output) VALUES ({0}, {1}, {2})'.format(raw, output, advanced_output)

        conn = sqlite3.connect('text_cleansing.db')
        cursor = conn.cursor()
        cursor.execute('DROP TABLE text_cleansing')
        cursor.execute('CREATE TABLE IF NOT EXISTS text_cleansing (raw varchar(255) NOT NULL, basic_output VARCHAR(255) NOT NULL, advanced_output VARCHAR(255))')
        cursor.execute(query)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except:
        return False
    
#Dataframe text cleansing
def df_text_cleansing(df):
    tweet_clean = df.tweet.str.lower()
    tweet_clean = tweet_clean.apply(clean_text)
    df['tweet_clean']  = tweet_clean
    return df

#JSON Balikan text cleansing
# text cleansing
def text_cleansing_success(text, cleansed_text):
    return {
            'status_code': 200,
            'description': 'Sukses membersihkan text',
            'before' : text,
            'after': cleansed_text,
    }     

def text_cleansing_error():
    return{
            'status_code': 500,
            'description': 'Error menyimpan ke database',
    }   

#text cleansing file
def text_cleansing_file_error_column(columnLength):
    return {
         'status_code' : 500,
         'description' : 'File tidak sesuai {0} column. Harusnya 13'.format(columnLength),
    }

def text_cleansing_file_error():
    return {
            'status_code' : 500,
            'description' : 'File error',
    }

#SWAGGER URL
#text cleansing biasa
@swag_from('docs/text_cleansing.yml', methods=['POST'])
@app.route('/text-cleansing', methods=['POST'])
def text_cleansing():
    """Cleanse Text & Save it to sqllite"""
    #clean text
    text = request.form.get('text')
    cleansed_text = clean_text(text)
    #save to sql
    db = save_to_sqllite(text, cleansed_text)

    #generate balikan json
    json_response = object()
    if(db == True):
        json_response = text_cleansing_success(text, cleansed_text)
    else:
        json_response = text_cleansing_error()

    return jsonify(json_response)

#text cleansing advanced (remove stopwords & slang)
@swag_from('docs/text_cleansing_advanced.yml', methods=['POST'])
@app.route('/text-cleansing-advanced', methods=['POST'])
def text_cleansing_advanced():
    """Cleanse Text & Save it to sqllite (With additional function)"""
    #clean text
    text = request.form.get('text')
    cleansed_text = clean_text_advanced(text)
     #save to sql
    db = save_to_sqllite(text, cleansed_text)

    #generate balikan json
    json_response = object()
    if(db == True):
        json_response = text_cleansing_success(text, cleansed_text)
    else:
        json_response = text_cleansing_error()

    return jsonify(json_response)

#text cleansing dengan file upload
@swag_from('docs/text_cleansing_file.yml', methods=['POST'])
@app.route('/text-cleansing-file', methods=['POST'])
def text_cleansing_file():
    """Cleanse Text & Save it to sqllite (Output file)"""
    #get file
    file = request.files["file"]              

    #cek apakah tipe file sesuai
    if file and file.content_type  == 'application/vnd.ms-excel':     

        #baca dataframe
        df = pd.read_csv(file, encoding='latin-1')
        df.columns = map(str.lower, df.columns)

        #cek apakah kolom sesuai
        if(len(df.columns) != 13):
            return jsonify(text_cleansing_file_error_column(len(df.columns)))
        else:
            #cleansing text
            df = df_text_cleansing(df)
            data_output = df[['tweet', 'tweet_clean']]

            #save ke csv
            file_name = 'file_cleansing_{0}.csv'.format(datetime.now().strftime('%m%d%Y_%H%M%S'))
            data_output.to_csv('result/' + file_name)

            return send_file(
                'result/' + file_name,
                as_attachment = True,
                download_name= file_name,
            )
    else:
         return jsonify(text_cleansing_file_error())

@swag_from('docs/text_cleansing_file.yml', methods=['POST'])
@app.route('/text-cleansing-file-advanced', methods=['POST'])
def text_cleansing_file_advanced():
    """Cleanse Text & Save it to sqllite (Output file dengan slang dan stopwords)"""
     #get file
    file = request.files["file"] 

     #cek apakah tipe file sesuai         
    if file and file.content_type  == 'application/vnd.ms-excel':     

        #baca dataframe
        df = pd.read_csv(file, encoding='latin-1')
        df.columns = map(str.lower, df.columns)

        #cek apakah kolom sesuai
        if(len(df.columns) != 13):
              return jsonify(text_cleansing_file_error_column(len(df.columns)))
        else:
            #cleansing text 20 random. karena berat
            df = df_text_cleansing(df.sample(n=20, random_state=1))
            df['tweet_clean_advanced'] = df['tweet_clean'].apply(clean_text_advanced)
            data_output = df[['tweet', 'tweet_clean', 'tweet_clean_advanced']]

            #save ke csv
            file_name = 'file_cleansing_advanced_{0}.csv'.format(datetime.now().strftime('%m%d%Y_%H%M%S'))
            data_output.to_csv('result/' + file_name)

            return send_file(
                'result/' + file_name,
                as_attachment = True,
                download_name= file_name,
            )
    else:
        return jsonify(text_cleansing_file_error())

if __name__ == "__main__":
    # Development only: run "python main.py" and open http://localhost:8080
    # When deploying to Cloud Run, a production-grade WSGI HTTP server,
    # such as Gunicorn, will serve the app.
    app.run(host="127.0.0.1", port=8080, debug=True)
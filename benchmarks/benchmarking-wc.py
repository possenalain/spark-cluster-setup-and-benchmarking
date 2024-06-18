# %%
## Books keeping
# !pip install nltk -q

# imports
import nltk 
import re
import pandas as pd

from pathlib import Path
import warnings

import time

warnings.filterwarnings("ignore")

nltk.download("stopwords")
from nltk.corpus import stopwords

stop_words= stopwords.words("english")

# %%
# proprecessing

def preprocess_word(word):
    return re.sub("A-Za-z0-9]+"," ",word.lower())
def preprocess_words(words):
    preprocessed_words= [preprocess_word(word) for word in words.split()]
    return [word for word in preprocessed_words if word not in stop_words and word!=""]
    # return [word for word in preprocessed_words if word!=""]


# %%
benchmark_datasets=[ "enwiki-0.1-GB",
                        "enwiki-0.2-GB",
                        "enwiki-0.3-GB",
                        "enwiki-0.4-GB",
                        "enwiki-0.5-GB",
                        "enwiki-1-GB",    
                        "enwiki-1.5-GB",
                        "enwiki-2-GB",
                        "enwiki-3-GB",
                        "enwiki-5-GB",
                        "enwiki-10-GB",
                        "enwiki-25-GB"
                   ]

# %%
    
from pyspark import SparkConf,SparkContext


# %%
def generate_benchmark_configurations(benchmark):
    
    setups = []
    setups_names= ["setup1","setup2", "setup3"]
    cores = {"setup1":20, "setup2":8, "setup3":20}
    memory = {"setup1":"8g", "setup2":"8g", "setup3":"4g"}
    
    for setup in setups_names:
        setups.append({
                    "benchmark":benchmark,
                    "setup":setup,
                    "memory":memory[setup],
                    "cores":cores[setup]
                })
    return setups

# %%
def run_benchmark(setup,inbound_range = [100,120]):    
    # let it shine
    start = time.time()
    
    conf = SparkConf() \
            .setAppName(f"{setup['benchmark']}-app")  \
            .set("spark.executor.memory", setup['memory']) \
            .set("spark.executor.instances", "2")\
            .set("spark.executor.cores", f"{setup['cores']}")

    
    sc = SparkContext(conf = conf)
    
    path_to_file = Path(f"/opt/spark/apps/benchmarking-wc/data/enwiki-custom/{setup['benchmark']}.txt")
    df = sc.textFile(str(path_to_file))    
    words = df.flatMap(preprocess_words)
    words_count = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)
    word_count_sorted = words_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))
    word_count_sorted_filtered = word_count_sorted.filter(lambda x: x[1]>=inbound_range[0] and x[1]<=inbound_range[1])
    word_count_dict = word_count_sorted_filtered.collect()
    
    sc.stop()
    
    # timing
    time_taken = time.time() - start
    
    print(f'{setup["setup"]} - {setup["benchmark"]} : {time_taken:.2f}') 
    
    return time_taken


# %%
#run setups
def run_setups(benchmark , inbound_range = [100,120]):    
    
    setups = generate_benchmark_configurations(benchmark)
    
    for setup in setups:
        print(f"running {setup['setup']} - {setup['benchmark']}")
        try:
            setup["time_taken"] = round(run_benchmark(setup,inbound_range),2)
        except:
            setup["time_taken"] = -1
            
    return setups


# %%
# book keeping

def save_csv(data_df, save_dir = Path("results"),save_as = "results.csv"):
    Path(save_dir).mkdir(parents=True, exist_ok=True)                                                                                     
    try:                               
        data_df.to_csv(str(save_dir/save_as),index=False)
    except:
        print("error while saving")
    return data_df

def save_results(all_benchmarks_results, architecture_identifier = "standalone-no-docker"):    
    # save the results    
    
    save_dir = Path("/opt/spark/apps/benchmarking-wc/results")
    # create a dataframe
    df = pd.DataFrame(all_benchmarks_results)
    save_csv(df , save_dir = save_dir, save_as = f"{architecture_identifier}.csv")
    
    
    ## create a pivoted presentations
    pivot_df = df.pivot_table(values='time_taken', index='benchmark', columns='setup', aggfunc='sum').reset_index()
    save_csv(pivot_df , save_dir = save_dir, save_as = f"{architecture_identifier}-summary.csv")

# %%
# do the job for every benchmark

# 1 2 1  :: master 2 workers 1 history server
architecture_identifier = "standalone-with-docker-131"
all_benchmarks_results = []

for benchmark in benchmark_datasets:
    print(f"running for {benchmark}")
    
    try:
        benchmark_results = run_setups(benchmark = benchmark, 
                                inbound_range = [100,120])
    except:
        continue
    
    all_benchmarks_results.extend(benchmark_results)
    #save results constantly
    save_results(all_benchmarks_results, architecture_identifier = architecture_identifier )    
    print("*"*30)

print("finished")
# %%
# # handle the display
# from IPython.core.display import HTML
# display(HTML("<style>pre { white-space: pre !important; }</style>"))

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


# %%
benchmark_datasets=[ "0.1-GB",
                        "0.2-GB",
                        "0.3-GB",
                        "0.4-GB",
                        "0.5-GB",
                        "1-GB",    
                        "1.5-GB",
                        "2-GB",
                        "3-GB",
                        "5-GB",
                        "10-GB",
                        "25-GB"
                   ]

# %%
    
from pyspark import SparkConf,SparkContext


# conf = SparkConf().\
#         setMaster("local[*]").
#         setAppName(f"{benchmark}-app")

# conf = SparkConf() \
#     .setAppName(f"{benchmark}-app") \
#     .setMaster("local[*]") \  # [*] uses all available cores
#     .set("spark.executor.memory", "1g") \  # Set the memory for each executor
#     .set("spark.executor.cores", "2") \    # Set the number of cores for each executor
#     .set("spark.executor.instances", "1") 




# %%
# get configurations 
def validate_configurations(configurations, 
                            Tcores = 12, 
                            Tmemory =12,
                            workers_max = 1,
                            worker_memory = 6,
                            worker_cores = 6
                            ):
    #validate memory     #validate cores
    configurations= dict(filter(lambda x: Tmemory >= int(x[1][0])*int(x[1][2])*workers_max, configurations.items()))
    configurations= dict(filter(lambda x: Tcores >= int(x[1][1])*int(x[1][2])*workers_max, configurations.items()))
    
    #validate worker memory
    configurations= dict(filter(lambda x: worker_memory > int(x[1][0]), configurations.items()))
    configurations= dict(filter(lambda x: worker_cores > int(x[1][1]), configurations.items()))
    
    return configurations

def get_configurations():
    #[memory, cores, executors_per_worker]
    # config = [1, 2, 3, 4, 5, 6, 7, 8]
    # memory = [4, 4, 1, 2, 1, 2, 1, 1]
    # cores = [4, 1, 4, 2, 1, 2, 1, 1]
    # executors_per_worker = [1, 1, 1, 1, 1, 2, 2, 4]
    
    
    config = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    memory = [4, 4, 1, 2, 1, 2, 1, 1, 1, 1, 2, 2, 3, 2, 12, 6, 4, 4]
    cores = [4, 1, 4, 2, 1, 2, 1, 1, 1, 1, 2, 2, 3, 2, 12, 6, 3, 4]
    executors_count = [1, 1, 1, 1, 1, 2, 2, 4, 12, 6, 6, 4, 3, 3, 1, 2, 3, 2]

    # create dictionary of zipped configurations
    config_dict = dict(zip(["config"+str(i) for i in config], zip(memory, cores, executors_count)))
    
    return config_dict
    


# %%
def generate_benchmark_configurations(benchmark):
        
    configurations = get_configurations()
    configurations = validate_configurations(configurations)
    
    print("*"*30)
    print(configurations)
    print("*"*30)
    
    rendered_configurations=[]
    for config, cf_d in configurations.items():
        rendered_configurations.append({
                    "benchmark":benchmark,
                    "config":config,
                    "memory": cf_d[0],
                    "cores": cf_d[1],
                    "executor_instances":cf_d[2]
                })
    return rendered_configurations 

# %%
def run_benchmark(config,inbound_range = [100,120]):    
        # let it shine
        start = time.time()

        conf = SparkConf() \
                .setAppName(f"{config['benchmark']}-app")  \
                .set("spark.executor.memory", f"{config['memory']}g") \
                .set("spark.executor.instances", config['executor_instances']) \
                .set("spark.executor.cores", config['cores']) #\
                # .setMaster(f"local[*]")


        sc = SparkContext(conf = conf)

        # path_to_file = Path(f"data/enwiki-custom/enwiki-{config['benchmark']}.txt")
        
        path_to_file = Path(f"/opt/spark/apps/benchmarking-wc/data/enwiki-custom/enwiki-{config['benchmark']}.txt")
        df = sc.textFile(str(path_to_file))    
        words = df.flatMap(preprocess_words)
        words_count = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)
        word_count_sorted = words_count.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))
        word_count_sorted_filtered = word_count_sorted.filter(lambda x: x[1]>=inbound_range[0] and x[1]<=inbound_range[1])
        word_count_dict = word_count_sorted_filtered.collect()

        sc.stop()

        # timing
        time_taken = time.time() - start

        print(f'{config["config"]} - {config["benchmark"]} : {time_taken:.2f}') 

        return time_taken


# %%
#run setups
def run_setups(benchmark , inbound_range = [100,120]):    
    
    configurations = generate_benchmark_configurations(benchmark)
    
    for config in configurations:
        print(f"running {config['config']} - {config['benchmark']}")
        try:
            config["time_taken"] = round(run_benchmark(config,inbound_range),2)
        except:
            config["time_taken"] = -1
            
    return configurations


# %%
# book keeping

def save_csv(data_df, save_dir = Path("results"), save_as = "results.csv"):
    Path(save_dir).mkdir(parents=True, exist_ok=True)                                                                                     
    try:                               
        data_df.to_csv(str(save_dir/save_as),index=False)
    except:
        print("error while saving")
    return data_df

def save_results(all_benchmarks_results,architecture_identifier = "standalone-no-docker"):    
    # save the results    
    
    # create a dataframe
    df = pd.DataFrame(all_benchmarks_results)
    save_dir = Path("/opt/spark/apps/benchmarking-wc/results")
    save_csv(df , save_dir = save_dir, save_as = f"{architecture_identifier}.csv")
    ## create a pivoted presentations
    pivot_df = df.pivot_table(values='time_taken', index='benchmark', columns='config', aggfunc='sum').reset_index()
    save_csv(pivot_df , save_dir = Path("results"), save_as = f"{architecture_identifier}-summary.csv")

# %%
# %%capture cap
# do the job for every benchmark


# run each benchmark 3 times 
# all_runs_benchmarks_results = []
run = 1
architecture_identifier = f"{run}-standalone-with-docker-131"

print(f'{"*"*25} - {architecture_identifier} - {"*"*25}')
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


# %%
# with open(f'results/{architecture_identifier}-logs.txt', 'w') as file:
#     file.write(cap.stdout)

# %%
# all_benchmarks_results



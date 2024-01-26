# imports
import ast  # for converting embeddings saved as strings back to arrays
import openai
import pandas as pd  # for storing text and embeddings data
import tiktoken  # for counting tokens
import os # for getting API token from env variable OPENAI_API_KEY
from scipy import spatial  # for calculating vector similarities for search

import sys
sys.path.append('/src')
from src.config import openaiApiKey,openaiUrl,openaiEmbeddingsUrl

# models
EMBEDDING_MODEL = "text-embedding-ada-002"
GPT_MODEL = "gpt-3.5-turbo"

# an example question about the 2022 Olympics
query = 'Which athletes won the gold medal in curling at the 2022 Winter Olympics?'

client = openai.AzureOpenAI(
        azure_endpoint=openaiUrl,
        api_key=openaiApiKey,
        api_version="2023-07-01-preview"
    )

# # 尝试直接询问，结果是GPT无法回答
# response = client.chat.completions.create(
#     messages=[
#         {'role': 'system', 'content': 'You answer questions about the 2022 Winter Olympics.'},
#         {'role': 'user', 'content': query},
#     ],
#     model=GPT_MODEL,
#     temperature=0,
# )

# print(response.choices[0].message.content)

# 读取emmbeddingsDemo中收集的数据
embeddings_path = "/src/data/winter_olympics_2022.csv"
df = pd.read_csv(embeddings_path)
# convert embeddings from CSV str type back to list type
df['embedding'] = df['embedding'].apply(ast.literal_eval)

embeddingsClient = openai.AzureOpenAI(
        azure_endpoint=openaiEmbeddingsUrl,
        api_key=openaiApiKey,
        api_version="2023-07-01-preview"
    )

# search function
def strings_ranked_by_relatedness(
    query: str,
    df: pd.DataFrame,
    relatedness_fn=lambda x, y: 1 - spatial.distance.cosine(x, y),
    top_n: int = 100
) -> tuple[list[str], list[float]]:
    """Returns a list of strings and relatednesses, sorted from most related to least."""
    query_embedding_response = embeddingsClient.embeddings.create(
        model=EMBEDDING_MODEL,
        input=query,
    )
    query_embedding = query_embedding_response.data[0].embedding
    strings_and_relatednesses = [
        (row["text"], relatedness_fn(query_embedding, row["embedding"]))
        for i, row in df.iterrows()
    ]
    strings_and_relatednesses.sort(key=lambda x: x[1], reverse=True)
    strings, relatednesses = zip(*strings_and_relatednesses)
    return strings[:top_n], relatednesses[:top_n]

# # examples
# strings, relatednesses = strings_ranked_by_relatedness("curling gold medal", df, top_n=5)
# for string, relatedness in zip(strings, relatednesses):
#     print(f"{relatedness=:.3f}")
#     print(string)

def num_tokens(text: str, model: str = GPT_MODEL) -> int:
    """Return the number of tokens in a string."""
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def query_message(
    query: str,
    df: pd.DataFrame,
    model: str,
    token_budget: int
) -> str:
    """Return a message for GPT, with relevant source texts pulled from a dataframe."""
    strings, relatednesses = strings_ranked_by_relatedness(query, df)
    introduction = 'Use the below articles on the 2022 Winter Olympics to answer the subsequent question. If the answer cannot be found in the articles, write "I could not find an answer."'
    question = f"\n\nQuestion: {query}"
    message = introduction
    for string in strings:
        next_article = f'\n\nWikipedia article section:\n"""\n{string}\n"""'
        if (
            num_tokens(message + next_article + question, model=model)
            > token_budget
        ):
            break
        else:
            message += next_article
    return message + question


def ask(
    query: str,
    df: pd.DataFrame = df,
    model: str = GPT_MODEL,
    token_budget: int = 1024*7 - 500,
    print_message: bool = False,
) -> str:
    """Answers a query using GPT and a dataframe of relevant texts and embeddings."""
    message = query_message(query, df, model=GPT_MODEL, token_budget=token_budget)
    if print_message:
        print(message)
    messages = [
        {"role": "system", "content": "You answer questions about the 2022 Winter Olympics."},
        {"role": "user", "content": message},
    ]
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0
    )
    response_message = response.choices[0].message.content
    print(response_message)
    return response_message

ask('Which athletes won the gold medal in curling at the 2022 Winter Olympics?',model='bigpt4')

ask('中国是否有参加2022冬奥会？',model='bigpt4')

ask('中国在2022冬奥会表现怎么样，主要从金牌总数，奖牌总数，和金牌总数国家排名，奖牌总数国家排名来说说。',model='bigpt4')
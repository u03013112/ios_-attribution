import openai

import sys
sys.path.append('/src')

from src.config import openaiApiKey,openaiUrl,openaiEmbeddingsUrl

def chat(messages):
    client = openai.AzureOpenAI(
            azure_endpoint=openaiUrl,
            api_key=openaiApiKey,
            api_version="2023-07-01-preview"
        )

    response = client.chat.completions.create(
        model='bigpt4',
        messages=messages,
        temperature=0,
    )

    # print(f"{response.choices[0].message.role}: {response.choices[0].message.content}")
    return response.choices[0].message.content

def embeddings(input):
    client = openai.AzureOpenAI(
            azure_endpoint=openaiEmbeddingsUrl,
            api_key=openaiApiKey,
            api_version="2023-07-01-preview"
        )

    embeddings = client.embeddings.create(
        model='text-embedding-ada-002',
        input=input
    )
                                
    # print(embeddings)
    return embeddings

def function():
    deployment = 'bigpt4'
    client = openai.AzureOpenAI(
            azure_endpoint=openaiUrl,
            api_key=openaiApiKey,
            api_version="2023-07-01-preview"
        )
    
    functions = [
        {
            "name": "get_current_weather",
            "description": "Get the current weather",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city and state, e.g. San Francisco, CA",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "description": "The temperature unit to use. Infer this from the users location.",
                    },
                },
                "required": ["location"],
            },
        }
    ]

    messages = [
        {"role": "system", "content": "Don't make assumptions about what values to plug into functions. Ask for clarification if a user request is ambiguous."},
        {"role": "user", "content": "What's the weather like today in Seattle?"}
    ]

    chat_completion = client.chat.completions.create(
        model=deployment,
        messages=messages,
        functions=functions,
    )
    print(chat_completion)

    import json

    def get_current_weather(request):
        """
        This function is for illustrative purposes.
        The location and unit should be used to determine weather
        instead of returning a hardcoded response.
        """
        location = request.get("location")
        unit = request.get("unit")
        return {"temperature": "22", "unit": "celsius", "description": "Sunny"}

    function_call = chat_completion.choices[0].message.function_call
    print(function_call.name)
    print(function_call.arguments)

    if function_call.name == "get_current_weather":
        response = get_current_weather(json.loads(function_call.arguments))

    messages.append(
        {
            "role": "function",
            "name": "get_current_weather",
            "content": json.dumps(response)
        }
    )

    function_completion = client.chat.completions.create(
        model=deployment,
        messages=messages,
        functions=functions,
    )

    print(function_completion.choices[0].message.content.strip())

if __name__ == '__main__':
    # print(chat([
    #     {"role": "system", "content": "Don't make assumptions about what values to plug into functions. Ask for clarification if a user request is ambiguous."},
    #     {"role": "user", "content": "What's the weather like today in Seattle?"}
    # ]))
    # function()

    print(embeddings('hello world'))
    
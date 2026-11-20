# Sentiment analysis with Docker Model Runner

`reviews.py` sends each restaurant review to a local LLM and saves the results in DuckDB.

Requires Docker Desktop 4.40 or newer.

## 1. Enable Docker Model Runner

In Docker Desktop: **Settings → AI → Enable Docker Model Runner**, then **Apply**.

Or from a terminal:

```
docker desktop enable model-runner
```

## 2. Pull a Llama model

Models come from two places:

- **Docker Hub** – browse the catalog at https://hub.docker.com/u/ai (Llama 3.2: https://hub.docker.com/r/ai/llama3.2). Pull by name:

  ```
  docker model pull ai/llama3.2
  ```

- **Hugging Face** – browse GGUF models at https://huggingface.co/models?library=gguf. Pull with an `hf.co/` prefix plus the repo name:

  ```
  docker model pull hf.co/bartowski/Llama-3.2-3B-Instruct-GGUF
  ```

  To use it, set `DEFAULT_MODEL` in `reviews.py` to that model's name. The name shows up in `docker model list`.

Check that it downloaded:

```
docker model list
```

## 3. Interact Directly with the Model

```
docker model run ai/smollm2 "What is Docker?"
```

```
Docker is a tool used to package and run applications and their dependencies, such as containers and images, in a virtual or physical machine. It's designed to make it easier to create, ship, and run software quickly and inexpensively, which can help businesses save on costs and time by reducing the complexity of deploying software in a traditional environment.
```

## 4. Expose the local API endpoint

The model is more useful for programmatic integrations if you make it accessible
via a local API endpoint. Docker Model Runner is equipped to do this.

In Docker Desktop: **Settings → AI → Enable host-side TCP support**, set the port to `12434`, then **Apply**.

Or from a terminal:

```
docker desktop enable model-runner --tcp 12434
```

Check that the API responds:

```
curl http://127.0.0.1:12434/engines/v1/models
```

## Run the script

This folder has its own Pipfile and virtualenv. Install and run from here:

```
cd sentiment
pipenv install
pipenv run python reviews.py
```

The script uses `ai/llama3.2`. To change the model, edit `DEFAULT_MODEL` in `reviews.py`.

## The API request

For each review, the script sends one POST request to Model Runner's OpenAI-compatible chat endpoint:

```
POST http://127.0.0.1:12434/engines/v1/chat/completions
```

The body for the first review is printed every time the script runs:

```json
{
  "model": "ai/llama3.2",
  "temperature": 0,
  "messages": [
    {"role": "system", "content": "You classify the overall sentiment of restaurant reviews. ..."},
    {"role": "user", "content": "The braised short rib was tender and well seasoned, ..."}
  ]
}
```

| Parameter | Required | Value sent | Purpose |
|---|---|---|---|
| `model` | Yes | `ai/llama3.2` | Which pulled model answers the request. Must match a name from `docker model list`. |
| `messages` | Yes | List of two messages | The conversation the model responds to. |
| `messages[].role` | Yes | `system`, then `user` | `system` sets the model's instructions; `user` is the input to respond to. |
| `messages[].content` | Yes | Prompt text / review text | The system message tells the model to answer with one word: positive, negative, or neutral. The user message is the review from the CSV. |
| `temperature` | No | `0` | Controls randomness (see below). |

The script reads the answer from `choices[0].message.content` in the response.

### Temperature

When a model writes its answer, it scores every possible next word and picks one. Temperature controls how it picks:

- **`0`** – always picks the highest-scoring word. The same review gets the same answer on every run.
- **Higher values (e.g. `0.7`–`1.0`)** – lower-scoring words get picked more often. Answers vary more from run to run, which suits creative writing.

If you leave it out, the server uses its own default, which is above 0. This script uses `0` because classification should be consistent: running it twice should give the same label for the same review.

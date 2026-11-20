# Sentiment analysis with Local and Hosted Models

`reviews.py` sends each restaurant review to a local LLM and saves the results in DuckDB.

Requires Docker Desktop 4.40 or newer.

## 1. Enable Docker Model Runner

In Docker Desktop: **Settings → AI → Enable Docker Model Runner**, then **Apply**.

Or from a terminal:

```
docker desktop enable model-runner
```

## 2. Pull a simple model

Models come from two places:

- **Docker Hub** – browse the catalog at https://hub.docker.com/u/ai (smollm2: https://hub.docker.com/r/ai/smollm2). Pull by name:

  ```
  docker model pull ai/smollm2
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
$ docker model run ai/smollm2 "What is Docker?"
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

The examples in this chapter rely on the `httpx` and `openai` packages. Be sure to install them in your local environment. Then run the review script:

```
python reviews.py
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

## Exercise: extract fields from maintenance requests

`reviews.py` reduces each text to one label. In this exercise you write a script, `maintenance.py`, that pulls several **structured fields** out of free text and loads them into DuckDB. No solution is provided. You build it yourself, starting from `reviews.py`.

### Input

`maintenance_requests.csv` holds 36 messages that tenants sent to building management:

| Column | Example |
|---|---|
| `request_id` | `1001` |
| `submitted_at` | `2026-11-02 07:14` |
| `tenant_name` | `Dana Whitfield` |
| `message` | `Hi, the kitchen sink in 4B has been leaking under the cabinet since Tuesday...` |

The building has floors **1–6** with units **A–D** on each floor (`1A` through `6D`). Some messages don't name a unit, some name a unit that doesn't exist, and some aren't repair requests at all.

### Output

For each message, ask the model to return a JSON object with exactly these keys:

```json
{"unit": "4B", "category": "plumbing", "urgency": "high", "summary": "Kitchen sink leaking under cabinet"}
```

| Key | Allowed values |
|---|---|
| `unit` | A real unit (`1A`–`6D`), or `null` if the message doesn't name one |
| `category` | `plumbing`, `electrical`, `appliance`, `hvac`, `pest`, `security`, `other` |
| `urgency` | `low`, `medium`, `high` |
| `summary` | Short English description, about 10 words |

Write the results to a DuckDB table named `requests` in `maintenance.duckdb`. Keep the original CSV columns, add the four extracted columns, and use `CHECK` constraints on `category` and `urgency`, as `reviews.py` does for `sentiment`.

### Steps

1. Copy `reviews.py` to `maintenance.py` and point it at the new CSV and database.
2. Rewrite the system prompt. List the keys, the allowed values, and what counts as `high`, `medium` and `low` urgency. Tell the model to use `other` for anything that isn't a repair.
3. Parse each answer with `json.loads()`.
4. Write a `validate()` function that checks the parsed result before it goes into the table. It should confirm the result is a dict with exactly the four keys, the values are allowed, and the unit exists in the building. Normalize the unit to uppercase so `3c` becomes `3C`.
5. Decide what to do with a response that fails validation. At minimum, store the row with `NULL` fields so the failure is visible, not silently dropped.
6. Query the table: count requests by `urgency` and `category`, with `high` first.

### Suggestions

- **Treat the model's output as untrusted input.** It will sometimes return invalid JSON, extra keys, or values that aren't in your lists. Never insert a response without validating it first.
- **Ask the server to enforce JSON.** Add `response_format` to the request payload with `"type": "json_schema"` and a JSON Schema for the four keys (use `enum` for `category` and `urgency`). Model Runner enforces this, so the reply is always valid JSON with the right keys. The simpler `{"type": "json_object"}` is **ignored** by Model Runner. A schema still can't check whether a unit exists, so keep `validate()`.
- **Don't put example values in the prompt.** If the prompt says `like "4B"`, a small model tends to answer `4B` for messages that don't name a unit. Describe the format instead (floor number + letter).
- **Retry once with the error.** When validation fails, append the model's answer and a user message describing what was wrong, then ask again. This fixes most bad answers.
- **Keep `temperature` at `0`** so reruns give the same results.
- **Print failures** with the reason, so you can see what went wrong.

### Things to look at when it runs

- Messages with no unit (the front door, the elevator, the laundry room). Does the model return `null`, or does it make something up?
- Request `1035` names unit `7A`, which doesn't exist. Does your validation catch it?
- Messages with two problems in one (`1022`, `1034`), the Spanish message (`1010`), and the vague one (`1031`).
- Are the categories and urgencies correct? Read through the table and find a few you disagree with. How would you measure accuracy across a larger dataset?

## OpenAI-compatible API parameters

Many LLM servers (Docker Model Runner, NVIDIA, OpenAI, vLLM, Ollama, and others) accept the same request format, so code written for one works on the others by changing the URL, key, and model name.

Every request is a POST with a JSON body. `model` and `messages` are required (see [The API request](#the-api-request)). Hosted services also require an API key in an `Authorization: Bearer <key>` header; Model Runner, running locally, does not.

The optional parameters below shape how the model generates its answer:

| Parameter | Type | Example | What it does |
|---|---|---|---|
| `temperature` | number | `0`, `0.7` | Randomness. Low values give focused, repeatable output; high values give more varied output. |
| `top_p` | number (0–1) | `0.9` | Nucleus sampling. The model picks only from the most likely tokens whose probabilities add up to this value. `0.9` drops the unlikely bottom 10%. Adjust this or `temperature`, not both. |
| `max_tokens` | integer | `200` | Cap on the length of the response, in tokens (a token is roughly ¾ of a word). Limits cost and runaway output. A response that hits the cap is cut off mid-sentence. |
| `seed` | integer | `42` | Fixes the random sampling so repeated requests tend to return the same output. Not guaranteed to be fully deterministic. |
| `stream` | boolean | `false` | If `true`, the response arrives token by token as server-sent events, as in a chat interface. If `false`, the full response arrives at once, which is simpler to parse in a script. |
| `response_format` | object | `{"type": "json_schema", ...}` | Forces the output to match a JSON Schema. See the maintenance exercise above. |
| `parallel_tool_calls` | boolean | `true` | Only applies when the request includes `tools` (functions the model can ask your code to run). Sets whether the model may request several tool calls in one response or only one at a time. |

Example body using several of these:

```json
{
  "model": "ai/llama3.2",
  "messages": [{"role": "user", "content": "Summarize this review in one sentence: ..."}],
  "temperature": 0.2,
  "max_tokens": 100,
  "seed": 42,
  "stream": false
}
```

## Hosted NVIDIA models (free)

NVIDIA hosts many open models (Llama, Nemotron, Mistral, DeepSeek, and others) behind an OpenAI-compatible API. Free accounts get trial credits, which is enough for class exercises.

1. Go to https://build.nvidia.com/ and sign in or create an NVIDIA account.
2. Open any model page (e.g. a Nemotron or Llama model) and click **Get API Key**, then **Generate Key**. Keys begin with `nvapi-`.
3. Copy the key and store it as an environment variable. Do not commit it to the repo.

   ```
   export NVIDIA_KEY="nvapi-..."
   ```

4. Run the example script, which sends an image and a question to a vision model:

   ```
   python nvidia-endpoint.py
   ```

The endpoint is `https://integrate.api.nvidia.com/v1`, so any OpenAI-compatible client works by changing `base_url`, `api_key`, and `model`. Each model page on build.nvidia.com shows its model ID and sample code. To point `reviews.py` or `maintenance.py` at NVIDIA instead of Docker Model Runner, swap the local URL for this endpoint, add an `Authorization: Bearer $NVIDIA_KEY` header, and set the model name.

"""
Query an NVIDIA-hosted model through its OpenAI-compatible API.

Model: nvidia/nemotron-3.5-super-vl-120b-a12b
Nemotron 3.5 has 124B parameters and supports text, image, and video input.

Requires NVIDIA_KEY env var (free key from https://build.nvidia.com/).

Prints the JSON body POSTed to the endpoint, then streams the model's reply.
"""


import json
import os
from openai import OpenAI

BASE_URL = "https://integrate.api.nvidia.com/v1"

# Request body, exactly as it is sent to POST {BASE_URL}/chat/completions.
payload = {
  "model": "nvidia/nemotron-3.5-super-vl-120b-a12b",
  "messages": [{"role": "user", "content": [
    {"type": "text", "text": "Look at the people on bicycles in this image. What are they doing?"},
    {"type": "image_url", "image_url": {"url": "https://s3.amazonaws.com/uvasds-systems/images/vuelta.jpg"}}]}],
  "temperature": 1,
  "top_p": 0.95,
  "max_tokens": 16384,
  "parallel_tool_calls": True,
  "seed": 42,
  "chat_template_kwargs": {"enable_thinking": True},  # NVIDIA-specific; turns on reasoning output
  "stream": True,
}

# The API key is sent as an "Authorization: Bearer ..." header, not in the body.
client = OpenAI(base_url=BASE_URL, api_key=os.environ["NVIDIA_KEY"])

# Show the request before sending it
print(f"=== REQUEST: POST {BASE_URL}/chat/completions ===")
print(json.dumps(payload, indent=2))

# The OpenAI client has no chat_template_kwargs argument, so pass it via
# extra_body, which merges it into the same top-level JSON shown above.
args = {k: v for k, v in payload.items() if k != "chat_template_kwargs"}
completion = client.chat.completions.create(
  **args, extra_body={"chat_template_kwargs": payload["chat_template_kwargs"]})

# Stream the reply: reasoning tokens first, then the answer
print("\n=== RESPONSE ===")
print("--- reasoning ---")
answer_started = False
for chunk in completion:
  if not chunk.choices:
    continue
  delta = chunk.choices[0].delta
  reasoning = getattr(delta, "reasoning_content", None)
  if reasoning:
    print(reasoning, end="", flush=True)
  if delta.content:
    if not answer_started:  # label the switch from reasoning to final answer
      print("\n\n--- answer ---")
      answer_started = True
    print(delta.content, end="", flush=True)
print()

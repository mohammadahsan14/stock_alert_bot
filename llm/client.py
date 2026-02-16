# llm/client.py

import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Create client once (reused across app)
_client = None


def get_client() -> OpenAI:
    global _client

    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY")

        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY not found in environment variables."
            )

        _client = OpenAI(api_key=api_key)

    return _client


def llm_text(
    prompt: str,
    model: str = "gpt-4.1-mini",
    temperature: float = 0.2,
    max_output_tokens: int = 400,
) -> str:
    """
    Send prompt to LLM and return plain text response.
    """

    client = get_client()

    response = client.responses.create(
        model=model,
        input=prompt,
        max_output_tokens=max_output_tokens,
    )

    return response.output_text.strip()
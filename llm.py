from langchain_google_genai import ChatGoogleGenerativeAI
from config import (
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE
)


def get_llm(
    model=DEFAULT_MODEL,
    temperature=DEFAULT_TEMPERATURE
):    
    
    llm = ChatGoogleGenerativeAI(model=model, temperature=temperature)
    return llm

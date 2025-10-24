# src/chatbot.py
import pandas as pd
from openai import OpenAI
from constants import OPENAI_API_KEY, ORGANIZATION_ID

API_KEY = OPENAI_API_KEY
organization_id = ORGANIZATION_ID

class ChatBot(API_KEY, organization_id):
    def __init__(self):
        self.API_KEY = API_KEY
        self.organization_id = organization_id
    

    def _create_client(self):
        client = OpenAI(api_key=API_KEY, organization=organization_id)
        return client

    def get_insights(self, df : pd.DataFrame):
        client = self._create_client()
        resp = client.responses.create(
            model="gpt-4o",
            input=[
                {"role": "system", "content": "Você é um assistente técnico e conciso."},
                {"role": "user", "content": "Teste: me responda em uma frase."}
            ],
        )
        return resp.output_text

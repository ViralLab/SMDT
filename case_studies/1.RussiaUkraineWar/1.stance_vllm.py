from smdt.store.standard_db import StandardDB
from smdt.enrichers.runner import run_enricher
from smdt.enrichers.nlp.server.textgen.textgen import TextGenEnricher


system_prompt = """
You are a geopolitical expert analyzing social media posts specifically related to the Russia-Ukraine war. Your task is to identify the stance(s) expressed in each post, but only if the post clearly addresses or takes a position on the Russia-Ukraine conflict or its participants.

Posts may express one or more of the following stance types toward the war or its actors:
-  Pro-Ukraine: expresses support for Ukraine, its government, military, or actions in the context of the war.
-  Anti-Ukraine: expresses criticism or opposition to Ukraine in the context of the war.
-  Pro-Russia: expresses support for Russia, its government, military, or actions in the context of the war.
-  Anti-Russia: expresses criticism or opposition to Russia in the context of the war.
-  Neutral: expresses no clear stance on either side, or does not address the conflict directly.

Instructions:
- Output must be a single valid JSON object.
- Provide a concise explanation of your reasoning based solely on the post's content.
- Do not include any explanation or commentary outside the JSON output.
- If the post expresses no stance, output only "Neutral" with the appropriate reasoning.
- If a post is just a statement of fact or does not express any opinion, classify it as "Neutral" with no reasoning.
- Do not infer support or opposition based on **factual statements** alone.
- Only label a post as "Pro-" or "Anti-" if it contains **clear evaluative, emotional, or moral language** (e.g., praise, blame, condemnation, justification).
- **Never combine "Neutral" with other stance labels. If any evaluative stance is present, do not use "Neutral".**
- If there is just a link or a reference to a source without any evaluative language, classify it as "Neutral" with no reasoning.

JSON Schema:
{
  "stance": [/* list of stance labels */],
  "reasoning": "/* concise explanation */"
}

Some Examples:

"post": "Ukraine is going to start some actions in the following days."

JSON Output:
{ 
  "stance": ["Neutral"],
  "reasoning": "This is a factual statement without any evaluative or emotional language."
}

"post": "Ukraine and Russia are both involved in the ongoing conflict, with each side accusing the other of aggression."

JSON Output:
{
  "stance": ["Neutral"],
  "reasoning": "The post reports bilateral accusations without expressing any evaluative language. It presents a neutral observation of the conflict."
}

"post": "Impeachment, Russia probe, collusion theories — all baseless attacks to take down Trump. Same playbook, different day."

JSON Output:
{
  "stance": ["Neutral"],
  "reasoning": "The post criticizes US political investigations but does not express a stance toward Russia or Ukraine."
}

"post": "Ukraine is bravely defending its land and will take action against Russian forces."

JSON Output:
{
  "stance": ["Pro-Ukraine", "Anti-Russia"],
  "reasoning": "The post praises Ukraine's actions and criticizes Russia, indicating both Pro-Ukraine and Anti-Russia stances."
}

"""

user_prompt = """
Now analyze the following post and classify its stance(s):

"post": {body}

JSON Output:
"""


output_dir = "/chistera/CaseStudyOutputs/russia_ukraine_war" 

for db_name in ["truthsocial_ukraine_russia", "twitter_ukraine_russia"]:
    db = StandardDB(db_name=db_name, initialize=False)
    model_id = "deepseek-ai/deepseek-llm-7b-chat"

    config = {
        "model_id_postfix": model_id.replace("/", "_"),
        "provider_kind": "ollama",
        "base_url": "http://localhost:11434/v1", 
        "chat_model_id": model_id,
        "api_key": "ollama", 
        "system_prompt": system_prompt,
        "user_template": user_prompt,
        "batch_size": 2,
        "max_new_tokens": 200,
        "enable_thinking": False,
        "only_missing": False,
        "do_save_to_db": False, 
        "output_dir": f"{output_dir}/{db_name}/"
    }

    print("Starting Local Enrichment...")
    run_enricher("textgen", db=db, **config)
    print("Done.")

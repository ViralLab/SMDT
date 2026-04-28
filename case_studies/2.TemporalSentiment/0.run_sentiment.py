from smdt.store.standard_db import StandardDB
from smdt.enrichers.nlp.local.bert_clf.hf_bert_family_sentence_clf import (
    BERTSentenceClfConfig,
    BERTSentenceClfEnricher,
)

db_name2model_id = {
    "turkish_election2023": "VRLLab/TurkishBERTweet-Lora-SA",
    "twitter_usc2": "finiteautomata/bertweet-base-sentiment-analysis",
    "truthsocial_usc": "finiteautomata/bertweet-base-sentiment-analysis"
}

for db_name in db_name2model_id:
    model_id = db_name2model_id[db_name]
    OUTPUT_DIR = f"/chistera/CaseStudyOutputs/{db_name}_enrichments/sentiment/"
    DB_BATCH_SIZE = 2**2
    MODEL_BATCH_SIZE = 2**2

    db = StandardDB(db_name=db_name, initialize=False)
    
    if "turkish" in db_name:
        hf_peft_adapter_id = "VRLLab/TurkishBERTweet-Lora-SA"
    else:
        hf_peft_adapter_id = None
        
    config  = {
        "hf_model_id": model_id,
        "hf_peft_adapter_id": hf_peft_adapter_id,
        "model_batch_size": MODEL_BATCH_SIZE,
        "do_save_to_db": False,
        "output_dir": OUTPUT_DIR,
        "model_name": model_id.split("/")[-1],
        "only_missing": False,
        "reset_cache": False,
        "max_seq_len": 128,
    }

    enricher = BERTSentenceClfEnricher(db, config=config)
    enricher.run(db_batch_size=DB_BATCH_SIZE)

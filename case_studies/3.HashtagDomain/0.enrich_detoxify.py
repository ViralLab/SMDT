from smdt.store.standard_db import StandardDB
from smdt.enrichers.nlp.local.detoxify.detoxify import DetoxifyConfig, DetoxifyToxicityEnricher

output_dir = "/chistera/CaseStudyOutputs/HashtagDodetoxify_outputs/" 

for db_name in ["ArchiverTwitterV1", "TruthSocial"]:
    db = StandardDB(db_name=db_name, initialize=False)

    config = DetoxifyConfig(
        model_name="multilingual",
        model_batch_size=16,
        do_save_to_db=False,
        output_dir=f"{output_dir}/{db_name}",
    )

    enricher = DetoxifyToxicityEnricher(db, config=config)
    enricher.run()
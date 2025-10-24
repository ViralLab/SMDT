import pytest, psycopg

MODELS = True
try:
    from smdt.store.models.hashmap import HashMap
except Exception:
    MODELS = False


@pytest.mark.skipif(not MODELS, reason="HashMap model not importable")
def test_hash_map_unique(conn, now):
    h1 = HashMap(hash_key="K1", hash_value="V1", created_at=now)
    h2 = HashMap(hash_key="K1", hash_value="V2", created_at=now)
    cols = h1.insert_columns()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO hash_map ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
            h1.insert_values(),
        )
    conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO hash_map ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
                h2.insert_values(),
            )
    conn.rollback()

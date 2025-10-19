from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "reddit123")
)

with driver.session() as session:
    result = session.run("RETURN 'Connected!' as msg")
    print(result.single()["msg"])

driver.close()
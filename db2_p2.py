from neo4j import GraphDatabase, basic_auth
import pandas as pd

# Establish Neo4j connection
driver = GraphDatabase.driver(
    "bolt://44.203.255.135:7687",
    auth=basic_auth("neo4j", "lumber-manufacturer-mop")
)

# Read data from CSV files
customer_data = pd.read_csv("customer.csv")
item_data = pd.read_csv("item.csv")
vendor_data = pd.read_csv("vendor.csv")
vendor_item_data = pd.read_csv("vendor_item.csv")
store_data = pd.read_csv("store.csv")
orders_data = pd.read_csv("orders.csv")
order_item_data = pd.read_csv("order_item.csv")
employee_data = pd.read_csv("employee.csv")
contract_data = pd.read_csv("contract.csv")
oldprice_data = pd.read_csv("oldprice.csv")

# Create nodes for each table
with driver.session() as session:
    for index, row in customer_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (c:CUSTOMER $props)", props=properties)

    for index, row in item_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (i:ITEM $props)", props=properties)

    for index, row in vendor_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (v:VENDOR $props)", props=properties)

    for index, row in vendor_item_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (vi:VENDOR_ITEM $props)", props=properties)

    for index, row in store_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (s:STORE $props)", props=properties)

    for index, row in orders_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (o:ORDERS $props)", props=properties)

    for index, row in order_item_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (oi:ORDER_ITEM $props)", props=properties)

    for index, row in employee_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (e:EMPLOYEE $props)", props=properties)

    for index, row in contract_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (ct:CONTRACT $props)", props=properties)

    for index, row in oldprice_data.iterrows():
        properties = dict(row)
        session.run(f"CREATE (op:OLDPRICE $props)", props=properties)

# Create relationships
with driver.session() as session:
    for index, row in vendor_item_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:VENDOR {{vId: $start_id}}), (end:ITEM {{iId: $end_id}}) "
            f"CREATE (start)-[:SELLS $rel_props]->(end)",
            start_id=start_node_props['vId'], end_id=end_node_props['iId'], rel_props=rel_props
        )

    for index, row in vendor_item_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:VENDOR_ITEM {{vId: $start_id}}), (end:VENDOR {{vId: $end_id}}) "
            f"CREATE (start)-[:FOUND_IN $rel_props]->(end)",
            start_id=start_node_props['vId'], end_id=end_node_props['vId'], rel_props=rel_props
        )

    for index, row in orders_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)

        session.run(
            f"MATCH (start:ORDERS {{oId: $start_id}}), (end:CUSTOMER {{cId: $end_id}}) "
            f"CREATE (start)-[:BELONGS_TO]->(end)",
            start_id=start_node_props['oId'], end_id=end_node_props['cId']
        )


    for index, row in order_item_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:ORDERS {{oId: $start_id}}), (end:ITEM {{iId: $end_id}}) "
            f"CREATE (start)-[:CONTAINS $rel_props]->(end)",
            start_id=start_node_props['oId'], end_id=end_node_props['iId'], rel_props=rel_props
        )

    for index, row in order_item_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:ORDER_ITEM {{oId: $start_id}}), (end:ITEM {{iId: $end_id}}) "
            f"CREATE (start)-[:MATCHES $rel_props]->(end)",
            start_id=start_node_props['oId'], end_id=end_node_props['iId'], rel_props=rel_props
        )

    for index, row in employee_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:EMPLOYEE {{sId: $start_id}}), (end:STORE {{sId: $end_id}}) "
            f"CREATE (start)-[:WORKS_AT $rel_props]->(end)",
            start_id=start_node_props['sId'], end_id=end_node_props['sId'], rel_props=rel_props
        )

    for index, row in contract_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:VENDOR {{vId: $start_id}}), (end:CONTRACT {{vId: $end_id}}) "
            f"CREATE (start)-[:HAS $rel_props]->(end)",
            start_id=start_node_props['vId'], end_id=end_node_props['vId'], rel_props=rel_props
        )

    for index, row in oldprice_data.iterrows():
        start_node_props = dict(row)
        end_node_props = dict(row)
        rel_props = dict(row)

        session.run(
            f"MATCH (start:ITEM {{iId: $start_id}}), (end:OLDPRICE {{iId: $end_id}}) "
            f"CREATE (start)-[:HAD $rel_props]->(end)",
            start_id=start_node_props['iId'], end_id=end_node_props['iId'], rel_props=rel_props
        )
#Query and print data
query = """
MATCH (c:CUSTOMER) RETURN c;
"""
with driver.session() as session:
    result = session.run(query)
    print("data:")
    for record in result:
        print(record)

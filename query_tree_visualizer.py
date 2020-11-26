import psycopg2
from treelib import Node, Tree

# set up DB connection
hostname = 'localhost'
username = 'postgres'
password = 'password'
database = 'postgres'

# retrieve records from database
def queryGetPlanTrees(conn):
    cur = conn.cursor()

    cur.execute(
        """
        select planno, parentid, id, name, cost, card 
        from picassoplantree
        order by planno asc, parentid asc, id asc
        """
    )

    result_list = []
    for planno, parentid, id, name, cost, card in cur.fetchall():
        row = {
            "planno": planno,
            "parentid": parentid,
            "id": id,
            "name": name,
            "cost": cost,
            "card": card
        }
        result_list.append(row)
    
    return result_list

# connect to database
myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

# retrieve records from database
results = queryGetPlanTrees(myConnection)

# close databbase connection
myConnection.close()

# initiate tree list
query_tree_list = []

# get unique planno from records in ascending order
planno_id_list_asc = list(set(dic["planno"] for dic in results))
planno_id_list_asc.sort()

# looping through set of records by each planno at a time
for planno in planno_id_list_asc:
    # filter to get records with matching planno
    rows_in_plan = list(filter(lambda d: d['planno'] in [planno], results))

    # initiate a tree object
    tree = Tree()

    # get unique parentnode from records in this record set in ascending order
    parentnode_id_list_asc = list(set(dic["parentid"] for dic in rows_in_plan))
    parentnode_id_list_asc.sort()

    # looping through set of records by each parentnode at a time
    for parentnode_id in parentnode_id_list_asc:
        # filter to get records with matching parentnode
        filtered_rows = list(filter(lambda d: d['parentid'] in [parentnode_id], rows_in_plan))

        # looping through each record to generate node for tree
        for node in filtered_rows:

            # instantiating node_id, node_tag (display name) for each record for readability
            node_id = node["id"]
            if node["cost"] == 0:
                node_tag = "{name}".format(name=node["name"])
            else:
                node_tag = "{name} (cost: {cost}, card: {card})".format(name=node["name"], cost=node["cost"], card=node["card"])

            # create node from record and append to tree
            if parentnode_id == -1:
                tree.create_node(node_tag, node_id)
            else:
                node_parent_id = node["parentid"]
                tree.create_node(node_tag, node_id, parent=node_parent_id)

    # append tree to tree list
    query_tree_list.append(tree)

# print and display trees
for tree in query_tree_list:
    tree.show()
